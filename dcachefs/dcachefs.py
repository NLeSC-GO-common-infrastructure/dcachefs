import aiohttp
import asyncio
import logging
import weakref

from datetime import datetime
from fsspec.asyn import sync_wrapper, sync, AsyncFileSystem
from fsspec.implementations.http import get_client, HTTPFile, HTTPStreamFile
from fsspec.utils import DEFAULT_BLOCK_SIZE
from urllib.parse import quote
from urlpath import URL

logger = logging.getLogger(__name__)


DCACHE_FILE_TYPES = {
    'REGULAR': 'file',
    'DIR': 'directory'
}


def _get_details(path, data):
    """
    Extract details from the metadata returned by the dCache API

    :param path: (str) file or directory path
    :param data: (dict) metadata as provided by the API
    :return (dict) parsed metadata
    """
    path = URL(path)

    name = data.get('fileName')  # fileName might be missing
    name = path/name if name is not None else path
    name = name.path
    element_type = data.get('fileType')
    element_type = DCACHE_FILE_TYPES.get(element_type, 'other')
    created = data.get('creationTime')  # in ms
    created = datetime.fromtimestamp(created / 1000.)
    modified = data.get('mtime')  # in ms
    modified = datetime.fromtimestamp(modified / 1000.)
    return dict(
        name=name,
        size=data.get('size'),
        type=element_type,
        created=created,
        modified=modified
    )


def _encode(path):
    return quote(path, safe='')


class dCacheFileSystem(AsyncFileSystem):
    """
    File system interface for a dCache storage instance.

    Inspired by the fsspec HTTPFileSystem implementation, specific methods
    interacts with the dCache system either via its API or via the WebDAV
    protocol.

    Parameters
    ----------
    block_size: int
        Blocks to read bytes; if 0, will default to raw requests file-like
        objects
    client_kwargs: dict
        Passed to `aiohttp.ClientSession`, see
        https://docs.aiohttp.org/en/stable/client_reference.html
        For example, `{'auth': aiohttp.BasicAuth('user', 'pass')}`
    request_kwargs: dict
        Passed to the `request` method of `aiohttp.ClientSession` (also
        see `client_kwargs`)
    **storage_options: dict
        Passed to the super-class
    """

    def __init__(
        self,
        api_url=None,
        webdav_url=None,
        username=None,
        password=None,
        token=None,
        block_size=None,
        client_kwargs=None,
        request_kwargs=None,
        asynchronous=False,
        loop=None,
        batch_size=None,
        **storage_options
    ):
        super().__init__(
            self,
            asynchronous=asynchronous,
            loop=loop,
            batch_size=batch_size,
            **storage_options
        )
        self.api_url = api_url
        self.webdav_url = webdav_url
        self.client_kwargs = {} if client_kwargs is None else client_kwargs
        self.request_kwargs = {} if request_kwargs is None else request_kwargs
        if (username is None) ^ (password is None):
            raise ValueError('Username or password not provided')
        if (username is not None) and (password is not None):
            self.client_kwargs.update(
                auth=aiohttp.BasicAuth(username, password)
            )
        if token is not None:
            if password is not None:
                raise ValueError('Provide either token or username/password')
            headers = self.client_kwargs.get('headers', {})
            headers.update(Authorization=f'Bearer {token}')
            self.client_kwargs.update(headers=headers)
        block_size = DEFAULT_BLOCK_SIZE if block_size is None else block_size
        self.block_size = block_size
        self._session = None
        if not asynchronous:
            sync(self.loop, self.set_session)

    @staticmethod
    def close_session(loop, session):
        if loop is not None and loop.is_running():
            try:
                sync(loop, session.close, timeout=0.1)
                return
            except (TimeoutError, FSTimeoutError):
                pass
        connector = getattr(session, "_connector", None)
        if connector is not None:
            # close after loop is dead
            connector._close()

    async def set_session(self):
        if self._session is None:
            self._session = await get_client(
                loop=self.loop,
                **self.client_kwargs
            )
            if not self.asynchronous:
                weakref.finalize(
                    self,
                    self.close_session,
                    self.loop,
                    self._session
                )
        return self._session

    @property
    def api_url(self):
        if self._api_url is None:
            raise ValueError('dCache API URL not set!')
        return self._api_url

    @api_url.setter
    def api_url(self, api_url):
        self._api_url = api_url

    @property
    def webdav_url(self):
        if self._webdav_url is None:
            raise ValueError('WebDAV door not set!')
        return self._webdav_url

    @webdav_url.setter
    def webdav_url(self, webdav_url):
        self._webdav_url = webdav_url

    @classmethod
    def _strip_protocol(cls, path):
        """
        Turn path from fully-qualified to file-system-specific

        :param path: (str or list)
        :return (str)
        """
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        url = URL(path)
        return url.path if "http" in url.scheme else path.split(":/")[-1]

    @classmethod
    def _get_kwargs_from_urls(cls, path):
        """
        Extract kwargs encoded in the path
        :param path: (str)
        :return (dict)
        """
        webdav_url = cls._get_webdav_url(path)
        return {'webdav_url': webdav_url} if webdav_url is not None else {}

    @classmethod
    def _get_webdav_url(cls, path):
        """
        Extract kwargs encoded in the path(s)

        :param path: (str or list) if list, extract URL from the first element
        :return (dict)
        """
        if isinstance(path, list):
            return cls._get_webdav_url(path[0])
        url = URL(path)
        return url.drive if "http" in url.scheme else None

    async def _get_info(self, path, children=False, limit=None, **kwargs):
        """
        Request file or directory metadata to the API

        :param path: (str)
        :param children: (bool) if True, return metadata of the children paths
            as well
        :param limit: (int) if provided and children is True, set limit to the
            number of children returned
        :param kwargs: (dict) optional arguments passed on to requests
        :return (dict) path metadata
        """
        url = URL(self.api_url) / 'namespace' / _encode(path)
        url = url.with_query(children=children)
        if limit is not None and children:
            url = url.add_query(limit=f'{limit}')
        url = url.as_uri()
        request_kwargs = self.request_kwargs.copy()
        request_kwargs.update(kwargs)
        session = await self.set_session()
        async with session.get(url, **request_kwargs) as r:
            if r.status == 404:
                raise FileNotFoundError(url)
            r.raise_for_status()
            return await r.json()

    async def _ls(self, path, detail=True, limit=None, **kwargs):
        """
        List path content.

        :param path: (str)
        :param detail: (bool) if True, return a list of dictionaries with the
            (children) path(s) info. If False, return a list of paths
        :param limit: (int) set the maximum number of children paths returned
            to this value
        :param kwargs: (dict) optional arguments passed on to requests
        :return list of dictionaries or list of str
        """
        path = self._strip_protocol(path)

        info = await self._get_info(
            path,
            children=True,
            limit=limit,
            **kwargs
        )
        details = _get_details(path, info)
        if details['type'] == 'directory':
            elements = info.get('children') or []
            details = [_get_details(path, el) for el in elements]
        else:
            details = [details]

        if detail:
            return details
        else:
            return [d.get('name') for d in details]

    ls = sync_wrapper(_ls)

    async def _cat_file(self, url, start=None, end=None, **kwargs):
        webdav_url = self._get_webdav_url(url) or self.webdav_url

        path = self._strip_protocol(url)
        url = URL(webdav_url) / path
        url = url.as_uri()
        request_kwargs = self.request_kwargs.copy()
        request_kwargs.update(kwargs)
        if (start is None) ^ (end is None):
            raise ValueError("Give start and end or neither")
        if start is not None:
            headers = request_kwargs.pop("headers", {}).copy()
            headers["Range"] = "bytes=%i-%i" % (start, end - 1)
            request_kwargs["headers"] = headers
        session = await self.set_session()
        async with session.get(url, **request_kwargs) as r:
            if r.status == 404:
                raise FileNotFoundError(url)
            r.raise_for_status()
            out = await r.read()
        return out

    async def _get_file(self, rpath, lpath, chunk_size=5 * 2 ** 20, **kwargs):
        webdav_url = self._get_webdav_url(rpath) or self.webdav_url

        path = self._strip_protocol(rpath)
        url = URL(webdav_url) / path
        url = url.as_uri()
        request_kwargs = self.request_kwargs.copy()
        request_kwargs.update(kwargs)
        session = await self.set_session()
        async with session.get(url, **request_kwargs) as r:
            if r.status == 404:
                raise FileNotFoundError(rpath)
            r.raise_for_status()
            with open(lpath, "wb") as fd:
                chunk = True
                while chunk:
                    chunk = await r.content.read(chunk_size)
                    fd.write(chunk)

    async def _put_file(self, lpath, rpath, **kwargs):
        webdav_url = self._get_webdav_url(rpath) or self.webdav_url

        path = self._strip_protocol(rpath)
        url = URL(webdav_url) / path
        url = url.as_uri()
        request_kwargs = self.request_kwargs.copy()
        request_kwargs.update(kwargs)
        session = await self.set_session()
        with open(lpath, "rb") as fd:
            r = await session.put(url, data=fd, **request_kwargs)
            r.raise_for_status()

    async def _cp_file(self, path1, path2, **kwargs):
        raise NotImplementedError

    async def _pipe_file(self, path1, path2, **kwargs):
        raise NotImplementedError

    async def _mv(self, path1, path2, **kwargs):
        """
        Rename path1 to path2

        :param path1: (str) source path
        :param path2: (str) destination path
        :param kwargs: (dict) optional arguments passed on to requests
        """
        path1 = self._strip_protocol(path1)
        path2 = self._strip_protocol(path2)

        url = URL(self.api_url) / 'namespace' / _encode(path1)
        url = url.as_uri()
        data = dict(action='mv', destination=path2)
        request_kwargs = self.request_kwargs.copy()
        request_kwargs.update(kwargs)
        session = await self.set_session()
        async with session.post(url, json=data, **request_kwargs) as r:
            if r.status == 404:
                raise FileNotFoundError(url)
            r.raise_for_status()
            return await r.json()

    mv = sync_wrapper(_mv)

    async def _rm_file(self, path, **kwargs):
        """
        Remove file or directory (must be empty)

        :param path: (str)
        """
        url = URL(self.api_url) / 'namespace' / _encode(path)
        url = url.as_uri()
        request_kwargs = self.request_kwargs.copy()
        request_kwargs.update(kwargs)
        session = await self.set_session()
        async with session.delete(url, **request_kwargs) as r:
            if r.status == 404:
                raise FileNotFoundError(url)
            r.raise_for_status()

    async def _rm(self, path, recursive=False, **kwargs):
        """
        Asynchronous remove method. Need to delete elements from branches
        towards root, awaiting tasks to be completed.
        """
        path = await self._expand_path(path, recursive=recursive)
        for p in reversed(path):
            await asyncio.gather(self._rm_file(p, **kwargs))

    rm = sync_wrapper(_rm)

    async def _info(self, path, **kwargs):
        """
        Give details about a file or a directory

        :param path: (str)
        :param kwargs: (dict) optional arguments passed on to requests
        :return (dict)
        """
        path = self._strip_protocol(path)
        info = await self._get_info(path, **kwargs)
        return _get_details(path, info)

    info = sync_wrapper(_info)

    def created(self, path):
        """
        Date and time in which the path was created

        :param path: (str)
        :return (datetime.datetime object)
        """
        return self.info(path).get('created')

    def modified(self, path):
        """
        Date and time in which the path was last modified

        :param path: (str)
        :return (datetime.datetime object)
        """
        return self.info(path).get('modified')

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        request_kwargs=None,
        **kwargs
    ):
        """Make a file-like object

        Parameters
        ----------
        path: str
            Full URL with protocol
        mode: string
            must be "rb"
        block_size: int or None
            Bytes to download in one request; use instance value if None. If
            zero, will return a streaming Requests file-like instance.
        kwargs: key-value
            Any other parameters, passed to requests calls
        """
        if mode not in {"rb", "wb"}:
            raise NotImplementedError
        block_size = self.block_size if block_size is None else block_size
        rkw = self.request_kwargs.copy()
        request_kwargs = {} if request_kwargs is None else request_kwargs
        rkw.update(request_kwargs)
        session = sync(self.loop, self.set_session)
        if block_size:
            return dCacheFile(
                self,
                path,
                mode=mode,
                block_size=block_size,
                request_kwargs=rkw,
                asynchronous=self.asynchronous,
                session=session,
                loop=self.loop,
                **kwargs
            )
        else:
            return dCacheStreamFile(
                self,
                path,
                mode=mode,
                request_kwargs=rkw,
                asynchronous=self.asynchronous,
                session=session,
                loop=self.loop,
                **kwargs
            )

    def open(
        self,
        path,
        mode="rb",
        **kwargs
    ):
        self.webdav_url = self._get_webdav_url(path) or self.webdav_url
        return super().open(
            path=path,
            mode=mode,
            **kwargs
        )


class dCacheFile(HTTPFile):
    """
    A file-like object pointing to a remove HTTP(S) resource

    Supports only reading, with read-ahead of a predermined block-size.

    In the case that the server does not supply the filesize, only reading of
    the complete file in one go is supported.

    Parameters
    ----------
    url: str
        Full URL of the remote resource, including the protocol
    session: requests.Session or None
        All calls will be made within this session, to avoid restarting
        connections where the server allows this
    block_size: int or None
        The amount of read-ahead to do, in bytes. Default is 5MB, or the value
        configured for the FileSystem creating this file
    size: None or int
        If given, this is the size of the file in bytes, and we don't attempt
        to call the server to find the value.
    kwargs: all other key-values are passed to requests calls.
    """

    def __init__(
        self,
        fs,
        url,
        mode="rb",
        block_size=None,
        request_kwargs=None,
        asynchronous=False,
        session=None,
        loop=None,
        **kwargs
    ):
        path = fs._strip_protocol(url)
        url = URL(fs.webdav_url) / path
        self.url = url.as_uri()
        self.asynchronous = asynchronous
        self.session = session
        self.loop = loop
        self.request_kwargs = {} if request_kwargs is None else request_kwargs
        if mode not in {"rb", "wb"}:
            raise ValueError
        super(HTTPFile, self).__init__(
            fs=fs,
            path=path,
            mode=mode,
            block_size=block_size,
            **kwargs
        )

    def flush(self, force=False):
        if self.closed:
            raise ValueError("Flush on closed file")
        if force and self.forced:
            raise ValueError("Force flush cannot be called more than once")
        if force:
            self.write_chunked()
            self.forced = True

    async def _write_chunked(self):
        self.buffer.seek(0)
        r = await self.session.put(
            self.url,
            data=self.buffer,
            **self.request_kwargs
        )
        async with r:
            r.raise_for_status()
        return False

    write_chunked = sync_wrapper(_write_chunked)

    def close(self):
        super(HTTPFile, self).close()


class dCacheStreamFile(HTTPStreamFile):
    def __init__(
        self,
        fs,
        url,
        mode="rb",
        request_kwargs=None,
        asynchronous=False,
        session=None,
        loop=None,
        **kwargs
    ):
        path = fs._strip_protocol(url)
        url = URL(fs.webdav_url) / path
        self.url = url.as_uri()
        self.details = {"name": self.url, "size": None}
        self.request_kwargs = {} if request_kwargs is None else request_kwargs
        self.asynchronous = asynchronous
        self.session = session
        self.loop = loop
        super(HTTPStreamFile, self).__init__(
            fs=fs,
            path=path,
            mode=mode,
            block_size=0,
            cache_type="none",
            **kwargs)
        if self.mode == "rb":

            async def get():
                r = await self.session.get(self.url, **self.request_kwargs)
                return r

            self.r = sync(self.loop, get)
            self.r.raise_for_status()
        elif self.mode == "wb":
            pass
        else:
            raise ValueError

    def write(self, data):
        if self.mode != "wb":
            raise ValueError("File not in write mode")

        async def put():
            r = await self.session.put(
                self.url,
                data=data,
                **self.request_kwargs
            )
            return r

        self.r = sync(self.loop, put)
        self.r.raise_for_status()

    def read(self, num=-1):
        if self.mode != "rb":
            raise ValueError("File not in read mode")
        return super().read(num=num)

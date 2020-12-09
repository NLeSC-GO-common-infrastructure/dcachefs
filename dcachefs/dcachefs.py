import datetime
import logging
import urllib
import urlpath

from fsspec.spec import AbstractFileSystem, AbstractBufferedFile


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
    """
    path = urlpath.URL(path)

    name = data.get('fileName')  # fileName might be missing
    name = path / name if name is not None else path
    name = name.path
    type = data.get('fileType')
    type = DCACHE_FILE_TYPES.get(type, 'other')
    created = data.get('creationTime')  # in ms
    created = datetime.datetime.fromtimestamp(created / 1000.)
    modified = data.get('mtime')  # in ms
    modified = datetime.datetime.fromtimestamp(modified / 1000.)
    return dict(
        name=name,
        size=data.get('size'),
        type=type,
        created=created,
        modified=modified
    )


def _encode(path):
    return urllib.parse.quote(path, safe='')


class dCacheFileSystem(AbstractFileSystem):
    """
    Interface for dCache

    It makes use of the API for all commands but reading/writing, for which the
    WebDAV door is employed
    """
    def __init__(
        self,
        api_url=None,
        username=None,
        password=None,
        token=None,
        webdav_url=None,
        **storage_options
    ):
        """
        :param api_url: (str) URL of the dCache API
        :param username: (str) to be provided with password
        :param password: (str) to be provided with username
        :param token: (str) macaroon for bearer token authentication
        :param webdav_url: (str) WebDAV door (only used for reading/writing)
        :param storage_options: (dict) additional arguments passed on to the
            super class
        """
        super().__init__(self, **storage_options)

        self._api_url = api_url

        if (username is None) ^ (password is None):
            raise ValueError('Username or password not provided')
        self.username = username
        self.password = password

        if password is not None and token is not None:
            raise ValueError('Provide either token or username/password')
        self.token = token

        self._webdav_url = webdav_url

    @property
    def api_url(self):
        if self._api_url is None:
            raise ValueError("dCache API URL not provided.")
        return urlpath.URL(self._api_url)

    @property
    def webdav_url(self):
        if self._webdav_url is None:
            raise ValueError("WebDAV URL not provided.")
        return urlpath.URL(self._webdav_url)

    @classmethod
    def _strip_protocol(cls, path):
        """
        Turn path from fully-qualified to file-system-specific

        :param path: (str)
        :return (str)
        """
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        return urlpath.URL(path).path

    @staticmethod
    def _get_kwargs_from_urls(path):
        """
        Extract kwargs encoded in the path

        :param path: (str)
        :return (dict)
        """
        kwargs = {}
        drive = urlpath.URL(path).drive
        if drive:
            kwargs.update(webdav_url=drive)
        return kwargs

    def _update_authentication_kwargs(self, kwargs):
        """
        Add authentication credentials to kwargs for requests

        :param kwargs: (dict)
        :return (dict)
        """
        if self.token is not None:
            headers = kwargs.get('headers', {})
            headers.update(Authorization=f'Bearer {self.token}')
            kwargs.update(headers=headers)
        if self.username is not None and self.password is not None:
            auth = (self.username, self.password)
            kwargs.update(auth=auth)
        return kwargs

    def _get_metadata(self, path, children=False, limit=None, **kwargs):
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
        path = self._strip_protocol(path)
        url = self.api_url / 'namespace' / _encode(path)
        url = url.with_query(children=children)
        if limit is not None and children:
            url = url.add_query(limit=f'{limit}')

        r = url.get(**self._update_authentication_kwargs(kwargs.copy()))
        r.raise_for_status()
        return r.json()

    def ls(self, path, detail=True, limit=None, **kwargs):
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
        metadata = self._get_metadata(path, children=True, limit=limit,
                                      **kwargs)
        elements = metadata.get('children') or [metadata]
        details = [_get_details(path, el) for el in elements]
        if detail:
            return details
        else:
            return [d.get('name') for d in details]

    def mv(self, path1, path2, **kwargs):
        """
        Rename path1 to path2

        :param path1: (str) source path
        :param path2: (str) destination path
        :param kwargs: (dict) optional arguments passed on to requests
        """

        path1 = self._strip_protocol(path1)
        url = self.api_url / 'namespace' / _encode(path1)

        path2 = self._strip_protocol(path2)
        data = dict(action='mv', destination=path2)

        r = url.post(json=data, **self._update_authentication_kwargs(kwargs))
        r.raise_for_status()

    def _rm(self, path):
        """
        Remove file or directory (must be empty)

        :param path: (str)
        """
        path = self._strip_protocol(path)
        url = self.api_url / 'namespace' / _encode(path)

        r = url.delete(**self._update_authentication_kwargs())
        r.raise_for_status()

    def info(self, path, **kwargs):
        """
        Give details about a file or a directory

        :param path: (str)
        :param kwargs: (dict) optional arguments passed on to requests
        :return (dict)
        """
        path = self._strip_protocol(path)
        metadata = self._get_metadata(path, **kwargs)
        return _get_details(path, metadata)

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
        autocommit=True,
        cache_options=None,
        **kwargs
    ):
        """Return raw bytes-mode file-like from the file-system"""
        return dCacheFile(
            fs=self,
            url=path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_options=cache_options,
            **kwargs
        )

    def open(
        self,
        path,
        mode="rb",
        block_size=None,
        cache_options=None,
        **kwargs
    ):
        options = self._get_kwargs_from_urls(path)
        self._webdav_url = options.get('webdav_url', self._webdav_url)
        return super().open(
            path=path,
            mode=mode,
            block_size=block_size,
            cache_options=cache_options,
            **kwargs
        )


class dCacheFile(AbstractBufferedFile):
    def __init__(self, fs, url, **kwargs):
        path = fs._strip_protocol(url)
        super().__init__(fs=fs, path=path, **kwargs)

    def _fetch_range(self, start, end):
        url = self.fs.webdav_url / self.path

        kwargs = self.kwargs.copy()
        headers = kwargs.pop('headers', {})
        headers.update(Range="bytes=%i-%i" % (start, end - 1))
        kwargs.update(headers=headers)
        kwargs = self.fs._update_authentication_kwargs(kwargs)

        r = url.get(**kwargs)

        if r.status_code == 416:
            # range request outside file
            return b""
        r.raise_for_status()
        if r.status_code == 206:
            # partial content, as expected
            out = r.content
        elif "Content-Length" in r.headers:
            cl = int(r.headers["Content-Length"])
            if cl <= end - start:
                # data size OK
                out = r.content
            else:
                raise ValueError(
                    f"Got more bytes ({cl}) than requested ({end - start})"
                )
        else:
            cl = 0
            out = []
            for chunk in r.iter_content(chunk_size=2 ** 20):
                # data size unknown, let's see if it goes too big
                if chunk:
                    out.append(chunk)
                    cl += len(chunk)
                    if cl > end - start:
                        raise ValueError(f"Got more bytes so far (>{cl})"
                                         f" than requested ({end - start})")
            out = b"".join(out)
        return out

    def _upload_chunk(self, final=False):
        url = self.fs.webdav_url / self.path

        data = self.buffer.getvalue()
        start = self.offset
        end = len(data)

        kwargs = self.kwargs.copy()
        headers = kwargs.pop('headers', {})
        headers.update(Range="bytes=%i-%i" % (start, end - 1))
        kwargs.update(headers=headers)
        kwargs = self.fs._update_authentication_kwargs(kwargs)

        r = url.put(data=data, **kwargs)
        r.raise_for_status()
        return True

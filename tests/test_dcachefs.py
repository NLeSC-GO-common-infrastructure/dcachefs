import datetime
import os
import pathlib
import pytest
import tempfile

from webdav3.client import Client

from dcachefs.dcachefs import dCacheFileSystem, dCacheFile, dCacheStreamFile


_file_content = 'Hello world!'


def _setup_test_dir(webdav_url, token):
    with tempfile.TemporaryDirectory() as tmpdirname:
        path = pathlib.Path(tmpdirname)
        root = path/'test'
        root.mkdir()
        for subdir in {'testdir_1', 'testdir_2', 'empty_testdir'}:
            subdir = root / subdir
            subdir.mkdir()
            if 'empty' in subdir.name:
                continue
            for file in {'file_1.txt', 'file_2.txt'}:
                file = root/subdir/file
                file.write_text(_file_content)
        client = Client(dict(webdav_hostname=webdav_url, webdav_token=token))
        client.upload(f'/{root.name}', root.as_posix())


@pytest.fixture(scope='session')
def test_fs():
    api_url = os.environ['DCACHE_API_URL']
    webdav_url = os.environ['DCACHE_WEBDAV_URL']
    token = os.environ['DCACHE_TOKEN']
    _setup_test_dir(webdav_url, token)
    return dCacheFileSystem(api_url=api_url,
                            token=token,
                            webdav_url=webdav_url)


def test_initialize_fs_without_password():
    with pytest.raises(ValueError):
        _ = dCacheFileSystem(
            username='user',
        )


def test_initialize_fs_with_both_auth_and_token():
    with pytest.raises(ValueError):
        _ = dCacheFileSystem(
            username='user',
            password='pass',
            token='test_token'
        )


def test_initialize_fs_without_api_url():
    fs = dCacheFileSystem(token='test_token')
    with pytest.raises(ValueError):
        fs.ls('/test')


def test_initialize_fs_without_webdav_url():
    fs = dCacheFileSystem(token='test_token')
    with pytest.raises(ValueError):
        fs.get('/test/test.txt', 'test.txt')


def test_ls_dir(test_fs):
    out = test_fs.ls('/test/testdir_1')
    assert len(out) == 2


def test_ls_file(test_fs):
    path = '/test/testdir_1/file_1.txt'
    out = test_fs.ls(path)
    assert len(out) == 1
    assert out[0]['name'] == path


def test_ls_empty_dir(test_fs):
    out = test_fs.ls('/test/empty_testdir')
    assert out == []


def test_ls_nonexistent_file(test_fs):
    path = '/test/testdir_2/nonexistent_file.txt'
    with pytest.raises(FileNotFoundError):
        test_fs.ls(path)


def test_info_dir(test_fs):
    out = test_fs.info('/test/testdir_1')
    assert out['type'] == 'directory'


def test_info_file(test_fs):
    out = test_fs.info('/test/testdir_1/file_1.txt')
    assert out['type'] == 'file'


def test_info_with_webdav_drive(test_fs):
    # the protocol and drive should be stripped by the path
    out = test_fs.info('https://webdav.com:9999/test/testdir_1/file_1.txt')
    assert out['type'] == 'file'


def test_info_nonexistent_file(test_fs):
    path = '/test/testdir_2/nonexistent_file.txt'
    with pytest.raises(FileNotFoundError):
        test_fs.info(path)


def test_rename_file(test_fs):
    old = '/test/testdir_2/file_1.txt'
    new = '/test/testdir_2/file_renamed.txt'
    test_fs.mv(old, new)
    assert not test_fs.exists(old)
    assert test_fs.exists(new)


def test_rename_nonexistent_file(test_fs):
    old = '/test/testdir_2/nonexistent_file.txt'
    new = '/test/testdir_2/file_renamed.txt'
    with pytest.raises(FileNotFoundError):
        test_fs.mv(old, new)


def test_remove_file(test_fs):
    path = '/test/testdir_2/file_2.txt'
    test_fs.rm(path)
    assert not test_fs.exists(path)


def test_remove_nonexistent_file(test_fs):
    path = '/test/testdir_2/nonexistent_file.txt'
    with pytest.raises(FileNotFoundError):
        test_fs.rm(path)


def test_created(test_fs):
    path = '/test/testdir_1/file_1.txt'
    out = test_fs.created(path)
    assert isinstance(out, datetime.datetime)


def test_modified(test_fs):
    path = '/test/testdir_1/file_1.txt'
    out = test_fs.modified(path)
    assert isinstance(out, datetime.datetime)


def test_cat(test_fs):
    path = '/test/testdir_1/file_1.txt'
    content = test_fs.cat(path)
    assert content == bytes(_file_content, 'utf-8')


def test_cat_with_range(test_fs):
    path = '/test/testdir_1/file_1.txt'
    content = test_fs.cat(path, start=6, end=11)
    assert content == b'world'


def test_cat_nonexistent_file(test_fs):
    path = '/test/testdir_2/nonexistent_file.txt'
    with pytest.raises(FileNotFoundError):
        _ = test_fs.cat(path)


def test_get(test_fs):
    remote_path = '/test/testdir_1/file_1.txt'
    with tempfile.TemporaryDirectory() as tmpdirname:
        local_path = pathlib.Path(tmpdirname) / 'tmp.txt'
        test_fs.get(remote_path, local_path.as_posix())
        assert local_path.is_file()
        with local_path.open() as f:
            assert f.read() == _file_content


def test_put(test_fs):
    remote_path = '/test/testdir_2/file_uploaded.txt'
    with tempfile.TemporaryDirectory() as tmpdirname:
        local_path = pathlib.Path(tmpdirname) / 'tmp.txt'
        with local_path.open(mode='w') as f:
            f.write(_file_content)
        test_fs.put(local_path.as_posix(), remote_path)
    assert test_fs.exists(remote_path)
    # cat returns binary output
    assert test_fs.cat(remote_path) == bytes(_file_content, 'utf-8')


def test_read_remote_file(test_fs):
    # the default mode is binary, caching `block_size` bytes retrieved with a
    # single get request
    remote_path = '/test/testdir_1/file_1.txt'
    with test_fs.open(remote_path) as f:
        assert isinstance(f, dCacheFile)
        assert f.read(5) == b'Hello'
        assert f.cache.data == bytes(_file_content, 'utf-8')
        f.read(1)
        assert f.read(5) == b'world'


def test_read_nonexistent_file(test_fs):
    remote_path = '/test/testdir_2/nonexistent_file.txt'
    with pytest.raises(FileNotFoundError):
        _ = test_fs.open(remote_path)


def test_read_remote_file_in_text_mode(test_fs):
    remote_path = '/test/testdir_1/file_1.txt'
    with test_fs.open(remote_path, 'r') as f:
        assert f.read() == _file_content


def test_read_remote_file_as_stream(test_fs):
    remote_path = '/test/testdir_1/file_1.txt'
    with test_fs.open(remote_path, block_size=0) as f:
        assert isinstance(f, dCacheStreamFile)
        assert f.read(5) == b'Hello'
        f.read(1)
        assert f.read(5) == b'world'


def test_write_remote_file(test_fs):
    remote_path = '/test/testdir_2/file_open.txt'
    with test_fs.open(remote_path, 'wb') as f:
        assert isinstance(f, dCacheStreamFile)
        f.write(_file_content)
    assert test_fs.exists(remote_path)
    # cat returns binary output
    assert test_fs.cat(remote_path) == bytes(_file_content, 'utf-8')


def test_write_remote_file_in_text_mode(test_fs):
    remote_path = '/test/testdir_2/file_open.txt'
    with test_fs.open(remote_path, 'w') as f:
        f.write(_file_content)
    assert test_fs.exists(remote_path)
    # cat returns binary output
    assert test_fs.cat(remote_path) == bytes(_file_content, 'utf-8')


def test_append_mode_is_not_supported(test_fs):
    remote_path = '/test/testdir_2/file_1.txt'
    with pytest.raises(NotImplementedError):
        _ = test_fs.open(remote_path, mode='a')

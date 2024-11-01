import pytest

from lazypp import Directory, File


def test_exceptions():
    with pytest.raises(ValueError):
        File("tests/data/hello1.txt", dest="../dest_hello.txt")


def test_hash():
    file1 = File("tests/data/hello1.txt")
    file2 = File("tests/data/hello2.txt")

    assert file1._xxh128_hash().hexdigest() != file2._xxh128_hash().hexdigest()

    dir1 = Directory("tests/data/foo1")
    dir2 = Directory("tests/data/foo2")

    assert dir1._xxh128_hash().hexdigest() != dir2._xxh128_hash().hexdigest()

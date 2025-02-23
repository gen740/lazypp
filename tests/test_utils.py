from lazypp.utils import run_sh


def test_run_sh():
    return_code = run_sh(["echo", "hello"])
    assert return_code == 0

    return_code = run_sh(["false"])
    assert return_code == 1

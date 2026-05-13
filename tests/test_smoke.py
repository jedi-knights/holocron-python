from holocron import __version__


def test_version_is_present() -> None:
    assert __version__
    assert isinstance(__version__, str)

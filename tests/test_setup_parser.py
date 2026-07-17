from github_innovation.github import parse_setup_requirements


def test_extracts_literal_install_requires():
    source = """
from setuptools import setup
requirements = ["httpx>=0.27", "PyYAML==6.0", "typing_extensions; python_version<'3.11'"]
setup(name="demo", install_requires=requirements)
"""
    assert parse_setup_requirements(source) == [
        ("httpx", "httpx>=0.27"),
        ("pyyaml", "PyYAML==6.0"),
        ("typing-extensions", "typing_extensions; python_version<'3.11'"),
    ]


def test_does_not_execute_or_evaluate_dynamic_code():
    source = "setup(install_requires=get_requirements())"
    assert parse_setup_requirements(source) == []

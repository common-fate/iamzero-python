# Contributing to iamzero-python

We welcome all contributions to IAM Zero. Please read our [Contributor Code of Conduct](https://github.com/common-fate/iamzero/blob/main/CODE_OF_CONDUCT.md).

## Getting set up

To run this project locally you will need:

- Python 3.6.2+
- [Poetry](https://python-poetry.org/docs/)

We've found it convenient to set up Poetry to create virtual environments in the same repository folder. You can set this as follows:

```bash
poetry config virtualenvs.in-project true
```

Install dependencies as follows:

```bash
poetry install
```

## Running tests

We use pytest to run tests for iamzero-python. To run pytest locally you can run:

```bash
poetry run pytest
```

# Contributions

Contributions are always welcome, as this is a new and active project.

## Reporting Issues & Feature Requests
If you encounter a bug, have a feature request, or want to suggest an improvement, please use our GitHub issue templates to streamline the process.

### Submitting an Issue

We use GitHub Issues to track bugs and feature requests. Before submitting a new issue, please:

- Check the existing issues to avoid duplicates.
- Ensure you're using the latest version of `pysqlscribe`, as the issue may have already been fixed.

### Issue Templates
We have predefined GitHub issue templates to help categorize and resolve issues efficiently:

- 🐞 Bug Report – If you’ve found a bug, provide clear reproduction steps and expected behavior.
- 🚀 Feature Request – Suggest new functionality or enhancements with a clear use case.
- 📖 Documentation Request – Report missing or unclear documentation.

You can open an issue using the appropriate template directly from our [Issues Page](https://github.com/danielenricocahall/pysqlscribe/issues/new/choose).


## Local Environment Setup
If you want to make a change yourself based on an Issue, it should be fairly simple after [forking the repository](https://github.com/danielenricocahall/pysqlscribe/fork) and cloning your fork. 

This project currently uses `uv` for convenience, although we currently only have dev dependencies. To create your environment:
```shell
uv sync
```

and install the `pre-commit` hook - we currently use `ruff` for linting and formatting:

```shell
uv run pre-commit install
```

## Unit Testing
`pytest` is used for all unit testing. To run the unit tests locally (assuming a local environment is set up):
```shell
uv run pytest
```

Unit tests are executed as part of CI, and the behavior should be consistent with what is observed in local development.

> 🚀 Looking for usage examples? See the [API Documentation](./README.md).
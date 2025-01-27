# Workflow Manager

This is a service that orchestrates an analysis pipeline.

To build:

`docker-compose --progress plain build --no-cache`

## Releasing a new version

1. Merge updates into the main branch
2. Create a new tag in main and name the tag: x.x.x following [semantic versioning](https://semver.org/).

    e.g ```git tag -a 0.0.1 -m "Initial release"```

    Given a version number MAJOR.MINOR.PATCH, increment the:

    - MAJOR version when you make incompatible API changes,
    - MINOR version when you add functionality in a backwards compatible manner, and
    - PATCH version when you make backwards compatible bug fixes.

3. Push the tag to GitHub

    eg. ```git push origin 0.0.1```
    
This will trigger Github Actions to create a new release with the same name.
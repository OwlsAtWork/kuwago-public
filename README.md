# Kuwago

## Branches

`main` branch of Kuwago contains all the Kuwago changes and the released Superset functionality.

### Updating Kuwago with the Superset Release

In order to get the new release of Superset:
- Get the latest tags in Superset
`git fetch --tags upstream`
- Create a new branch from `main` getting the desired `tag` of the Superset.
`git checkout -b <BRANCH_NAME> <TAG>`
i.e. Assuming the new release of Superset is 3.0.1 (with tag 3.0.1)
`git checkout -b release-3.0.1 3.0.1`
- Pull `main` to the current branch to resolve conflicts
`git pull origin main`
- Push the changes to the remote branch
`git push`
- Switch to the `main` branch

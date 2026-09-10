# Release notes

One file per tag, named after it: `v1.1.0` needs `docs/releases/v1.1.0.md`.

`.github/workflows/release.yml` reads that file and publishes it as the GitHub
release body. A tag pushed without it fails the release before anything is
built, so write and commit the notes first, then tag.

Worth covering, since a release is what production installs:

- what changed for someone deploying it, not a commit list;
- anything that changes the published data product, so a week-over-week
  difference is not read as a regression;
- new configuration keys, and whether they are required or optional.

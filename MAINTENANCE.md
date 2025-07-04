Releasing
=========
* Run the tests and ensure they all pass
* Update the version in ``cassandra/__init__.py``
* Add the new version in ``docs/conf.py`` (variables: ``TAGS``, ``LATEST_VERSION``, ``DEPRECATED_VERSIONS``).
   * For patch version releases (like ``3.26.8-scylla -> 3.26.9-scylla``) replace the old version with new one in ``TAGS`` and update ``LATEST_VERSION``.
   * For minor version releases (like ``3.26.9-scylla -> 3.27.0-scylla``) add new version to ``TAGS``, update ``LATEST_VERSION`` and add previous minor version to ``DEPRECATED_VERSIONS``.
* Commit the version changes, e.g. ``git commit -m 'Release 3.26.9'``
* Tag the release.  For example: ``git tag -a 3.26.9-scylla -m 'Release 3.26.9'``
* Push the tag and new ``master`` SIMULTANEOUSLY: ``git push --atomic origin master v6.0.21-scylla``
* Now new version and its docs should be automatically published. Check `PyPI <https://pypi.org/project/scylla-driver/#history>`_ and `docs <https://python-driver.docs.scylladb.com/stable/>`_ to make sure its there.
* If you didn't push branch and tag simultaneously (or doc publishing failed for other reason) then restart the relevant job from GitHub Actions UI.
* Publish a GitHub Release and a post on community forum.

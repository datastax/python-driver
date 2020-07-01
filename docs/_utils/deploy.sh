#!/bin/bash

# Clone repo
git clone "https://x-access-token:${GITHUB_TOKEN}@github.com/${GITHUB_REPOSITORY}.git" --branch gh-pages --single-branch gh-pages
cp -r docs/_build/dirhtml/* gh-pages
# Redirect index to master
cp docs/_utils/redirect.html gh-pages/index.html
# Deploy
cd gh-pages
touch .nojekyll
git config --local user.email "action@scylladb.com"
git config --local user.name "GitHub Action"
git add .
git commit -m "Publish docs" || true
git push origin gh-pages --force

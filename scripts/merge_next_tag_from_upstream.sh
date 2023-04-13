#! /bin/bash -e

# This script is helper for mergeing the next tag form upstream
# while re-tagging it so our Travis setup would pick it including our merge code
# otherwise if we just push the tags out of the upstream, they won't include the code from this fork

# this script assumes remotes for scylladb/python-driver and for datastax/python-driver are configured

upstream_repo_url=datastax/python-driver

upstream_repo=$(git remote -v | grep ${upstream_repo_url} | awk '{print $1}' | head -n1)
scylla_repo=$(git remote -v | grep scylladb/python-driver | awk '{print $1}' | head -n1)

git fetch ${upstream_repo}
git fetch ${scylla_repo}

scylla_tags=$(git ls-remote  --refs --tags --sort=v:refname ${scylla_repo} | awk '{print $2}')
upsteam_tags=$(git ls-remote  --refs --tags --sort=v:refname ${upstream_repo} | awk '{print $2}')

first_new_tag=$(diff -u <(echo "${scylla_tags}") <(echo "${upsteam_tags}") | grep '^\+' | grep -v '++\s' | sed -E 's/^\+//' | head -n1)


header="Merge branch '${first_new_tag}' of ${upstream_repo_url}"
commit_count=$(git log HEAD..${first_new_tag}  --oneline --pretty=tformat:'%h' | wc -l)
desc="* '${first_new_tag}' of {upstream_repo_url}: (${commit_count} commits)"
top20_commits=$(git log HEAD..${first_new_tag}  --oneline --pretty=tformat:'  %h: %s' | head -n20)

echo "
Preview of the merge:

$header

$desc
$top20_commits
  ..."


read -p "Continue with merge (y/n)?" choice
case "$choice" in
  y|Y )
      echo "Merging..."
      new_scyla_tag=$(echo ${first_new_tag} | sed 's|refs/tags/||')-scylla

      tag_msg="
   when done merging, use those to push a new tag:

      git merge --continue
      git tag ${new_scyla_tag}
      git push --tags ${scylla_repo} master

   re-triggering a build of a tag in Travis:

      git push --delete ${scylla_repo} ${new_scyla_tag}
      # then push it again
      git push ${scylla_repo} ${new_scyla_tag}

      "
      echo "$tag_msg"
      git pull https://github.com/datastax/python-driver ${first_new_tag} --log=20 --no-ff
      ;;


  * )
      echo "Aborted..."
      ;;
esac

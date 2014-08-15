from github import Github
import sys, os

g = Github(os.environ["GITHUB_TOKEN"])

milestone = sys.argv[1]
print "Fetching all issues for milestone %s" % milestone
repo = g.get_repo("cqlengine/cqlengine")

milestones = repo.get_milestones()
milestone = [x for x in milestones if x.title == milestone][0]
issues = repo.get_issues(milestone=milestone, state="closed")

for issue in issues:
    print "[%d] %s" % (issue.number, issue.title)

print("Done")


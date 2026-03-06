When the user runs `/commit`, generate a commit message and return it as executable git commands.

Format:

git add .
git commit -m "<type>(<scope>): <summary>" \
  -m "- change 1" \
  -m "- change 2"
git push

Rules:

- Use Conventional Commits
- Allowed types: feat, fix, refactor, perf, docs, test, chore, build, ci
- Each change must be its own `-m` bullet
- Use imperative verbs
- Do not include explanations
- Return only the command block
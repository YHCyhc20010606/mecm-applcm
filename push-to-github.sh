#!/usr/bin/env bash
set -euo pipefail

export PATH="/home/yhc/bin:${PATH:-}"
REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO_ROOT"

if ! gh auth status -h github.com &>/dev/null; then
  echo "请先登录 GitHub："
  echo "  gh auth login -h github.com -p https"
  exit 1
fi

GITHUB_URL="https://github.com/YHCyhc20010606/mecm-applcm.git"

if git remote get-url github &>/dev/null; then
  git remote set-url github "$GITHUB_URL"
else
  git remote add github "$GITHUB_URL"
fi

if ! gh repo view YHCyhc20010606/mecm-applcm &>/dev/null; then
  echo "在 GitHub 创建仓库 mecm-applcm ..."
  gh repo create YHCyhc20010606/mecm-applcm --private --push=false
fi

if [[ -f .git/shallow ]]; then
  echo "浅克隆补全历史（避免推送失败）..."
  git fetch origin --unshallow
fi

echo "推送到 GitHub ..."
git push -u github master

echo "完成: https://github.com/YHCyhc20010606/mecm-applcm"

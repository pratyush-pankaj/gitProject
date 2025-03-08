import argparse
import asyncio
import subprocess
import json
import os
import sys
import logging
import threading
import time
from typing import Dict, List, Optional

LOG_FILE = "git_events.json"
REPO_PATH = os.getcwd()
POLL_INTERVAL_DEFAULT = 5

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def run_git_command(args: List[str]) -> Optional[str]:
    try:
        result = subprocess.run(
            ["git"] + args,
            cwd=REPO_PATH,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logging.error(f"Git command error: {e.stderr}")
        return None

def get_current_branches() -> List[str]:
    output = run_git_command(["branch", "--format", "%(refname:short)"])
    return output.splitlines() if output else []

def get_latest_commit(branch: str) -> Optional[Dict]:
    output = run_git_command(["log", branch, "-1", "--pretty=format:%H|%ct|%s|%an"])
    if output:
        try:
            commit_hash, timestamp, message, author = output.split("|", 3)
            return {
                "hash": commit_hash,
                "timestamp": int(timestamp),
                "message": message,
                "author": author
            }
        except ValueError:
            logging.error("Error parsing commit output.")
    return None

def get_commit_files(commit_hash: str) -> List[str]:
    output = run_git_command(["diff-tree", "--no-commit-id", "--name-only", "-r", commit_hash])
    return output.splitlines() if output else []

def log_event(event: Dict):
    event["logged_at"] = int(time.time())
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(event) + "\n")
    except Exception as e:
        logging.error(f"Error writing to log file: {str(e)}")

async def monitor_repo(poll_interval: int = POLL_INTERVAL_DEFAULT):
    logging.info("Starting repository monitoring...")
    known_branches = set(get_current_branches())
    branch_commits = {}

    for branch in known_branches:
        commit = get_latest_commit(branch)
        if commit:
            branch_commits[branch] = commit["hash"]

    while True:
        try:
            current_branches = set(get_current_branches())
            new_branches = current_branches - known_branches
            for branch in new_branches:
                commit = get_latest_commit(branch)
                log_event({"event_type": "branch_creation", "branch": branch, "developer": commit["author"] if commit else "Unknown"})
                logging.info(f"Detected new branch: {branch}")
                if commit:
                    branch_commits[branch] = commit["hash"]
            known_branches = current_branches

            for branch in known_branches:
                commit = get_latest_commit(branch)
                if commit and branch in branch_commits and commit["hash"] != branch_commits[branch]:
                    files_changed = get_commit_files(commit["hash"])
                    log_event({
                        "event_type": "commit",
                        "branch": branch,
                        "commit_hash": commit["hash"],
                        "commit_message": commit["message"],
                        "commit_timestamp": commit["timestamp"],
                        "developer": commit["author"],
                        "files_changed": files_changed
                    })
                    logging.info(f"New commit on {branch}: {commit['hash']}")
                    branch_commits[branch] = commit["hash"]
        except Exception as e:
            logging.error(f"Error during monitoring: {str(e)}")
        await asyncio.sleep(poll_interval)

def monitor_push():
    hook_script = os.path.join(REPO_PATH, ".git", "hooks", "post-receive")
    with open(hook_script, "w", encoding="utf-8") as f:
        f.write('''"#!/bin/bash\n"
                "read oldrev newrev refname\n"
                "branch=$(echo $refname | sed 's#refs/heads/##')\n"
                "developer=$(git log -1 --pretty=format:'%an' $newrev)\n"
                "echo '{"event_type": "push", "branch": "'$branch'", "developer": "'$developer'", "timestamp": '$(date +%s)'}' >> "$(dirname "$0")/../../git_events.json"\n"''')
    os.chmod(hook_script, 0o755)

def start_monitoring(poll_interval: int):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(monitor_repo(poll_interval))
    except KeyboardInterrupt:
        logging.info("Monitoring stopped by user.")
    finally:
        loop.close()

def main():
    parser = argparse.ArgumentParser(description="Git Repository Monitoring CLI Tool")
    subparsers = parser.add_subparsers(dest="command")
    
    monitor_parser = subparsers.add_parser("monitor", help="Start monitoring the Git repository")
    monitor_parser.add_argument("--interval", type=int, default=POLL_INTERVAL_DEFAULT)
    monitor_parser.add_argument("--repo", type=str, default=os.getcwd())
    
    args = parser.parse_args()
    global REPO_PATH
    REPO_PATH = os.path.abspath(args.repo)

    if args.command == "monitor":
        monitor_push()
        monitor_thread = threading.Thread(target=start_monitoring, args=(args.interval,), daemon=True)
        monitor_thread.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Exiting monitoring mode.")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()

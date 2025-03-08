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
    """Executes a Git command in the configured REPO_PATH and returns the output."""
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
    """Retrieves the list of current branches in the repository."""
    output = run_git_command(["branch", "--format", "%(refname:short)"])
    return output.splitlines() if output else []

def get_latest_commit(branch: str) -> Optional[Dict]:
    """
    Retrieves the latest commit details for a given branch, including:
    - commit hash
    - timestamp
    - message
    - list of affected files
    Returns None if there are no commits or an error occurs.
    """
    output = run_git_command(["log", branch, "-1", "--pretty=format:%H|%ct|%s"])
    if output:
        try:
            commit_hash, timestamp, message = output.split("|", 2)
            # Get affected files
            files_output = run_git_command(["diff-tree", "--no-commit-id", "--name-only", "-r", commit_hash])
            affected_files = files_output.splitlines() if files_output else []
            return {
                "hash": commit_hash,
                "timestamp": int(timestamp),
                "message": message,
                "files": affected_files
            }
        except ValueError:
            logging.error("Error parsing commit output.")
    return None

def get_latest_push_event() -> Optional[Dict]:
    """
    Detects the latest push event using 'git reflog' and returns:
    - branch name
    - timestamp
    If no push is detected, returns None.
    """
    output = run_git_command(["reflog", "show", "--format=%gd|%ct", "--grep-reflog", "push"])
    if output:
        lines = output.splitlines()
        for line in lines:
            try:
                branch_info, timestamp = line.split("|")
                branch = branch_info.replace("HEAD@", "").strip()
                return {
                    "event_type": "push",
                    "branch": branch,
                    "timestamp": int(timestamp)
                }
            except ValueError:
                logging.error("Error parsing push event.")
    return None

def log_event(event: Dict):
    """Logs the event to git_events.json."""
    event["logged_at"] = int(time.time())
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(event) + "\n")
    except Exception as e:
        logging.error(f"Error writing to log file: {str(e)}")

async def monitor_repo(poll_interval: int = POLL_INTERVAL_DEFAULT):
    """Monitors the Git repository for new commits, branch creations, and push events."""
    logging.info("Starting repository monitoring...")
    
    known_branches = set(get_current_branches())
    branch_commits = {branch: get_latest_commit(branch)["hash"] for branch in known_branches if get_latest_commit(branch)}

    while True:
        try:
            current_branches = set(get_current_branches())

            # Detect new branches
            new_branches = current_branches - known_branches
            for branch in new_branches:
                event_data = {
                    "event_type": "branch_creation",
                    "branch": branch,
                    "timestamp": int(time.time())
                }
                log_event(event_data)
                logging.info(f"Detected new branch: {branch}")
                
                # Initialize commit tracking for new branch
                commit = get_latest_commit(branch)
                if commit:
                    branch_commits[branch] = commit["hash"]

            known_branches = current_branches

            # Detect new commits
            for branch in known_branches:
                commit = get_latest_commit(branch)
                if commit is None:
                    continue
                if branch not in branch_commits:
                    branch_commits[branch] = commit["hash"]
                    continue

                if commit["hash"] != branch_commits[branch]:
                    event_data = {
                        "event_type": "commit",
                        "branch": branch,
                        "commit_hash": commit["hash"],
                        "commit_message": commit["message"],
                        "commit_timestamp": commit["timestamp"],
                        "affected_files": commit["files"]
                    }
                    log_event(event_data)
                    logging.info(f"New commit on {branch}: {commit['hash']} | Files: {commit['files']}")
                    branch_commits[branch] = commit["hash"]

            # Detect push events
            push_event = get_latest_push_event()
            if push_event:
                log_event(push_event)
                logging.info(f"Detected push on branch {push_event['branch']}")

        except Exception as e:
            logging.error(f"Error during monitoring: {str(e)}")

        await asyncio.sleep(poll_interval)

def start_monitoring(poll_interval: int):
    """Starts the monitoring process in a separate thread with an asyncio event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(monitor_repo(poll_interval))
    except KeyboardInterrupt:
        logging.info("Monitoring stopped by user.")
    finally:
        loop.close()

def generate_report(developer: Optional[str] = None, event_type: Optional[str] = None,
                    start_date: Optional[int] = None, end_date: Optional[int] = None):
    """Generates a report from the log file based on the given filters."""
    if not os.path.exists(LOG_FILE):
        print("No log file found. Have you run 'monitor' yet?")
        return
    
    events = []
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if event_type and event.get("event_type") != event_type:
                    continue

                event_logged_at = event.get("logged_at", 0)
                if start_date and event_logged_at < start_date:
                    continue
                if end_date and event_logged_at > end_date:
                    continue

                if developer:
                    pass  # Placeholder for future developer filtering logic
                
                events.append(event)
    except Exception as e:
        logging.error(f"Error reading log file: {str(e)}")
        return
    
    print(json.dumps(events, indent=2))

def validate_repo_path(path: str) -> str:
    """Validates that the provided path is a Git repository."""
    abs_path = os.path.abspath(path)
    if not os.path.isdir(abs_path):
        logging.error("Provided repository path does not exist or is not a directory.")
        sys.exit(1)
    if not os.path.isdir(os.path.join(abs_path, ".git")):
        logging.error("Provided path is not a valid Git repository (missing .git folder).")
        sys.exit(1)
    return abs_path

def main():
    parser = argparse.ArgumentParser(description="Git Repository Monitoring CLI Tool")
    subparsers = parser.add_subparsers(dest="command", help="Sub-commands: monitor, report")
    
    monitor_parser = subparsers.add_parser("monitor", help="Start monitoring the Git repository")
    monitor_parser.add_argument("--interval", type=int, default=POLL_INTERVAL_DEFAULT, help="Polling interval in seconds")
    monitor_parser.add_argument("--repo", type=str, default=os.getcwd(), help="Path to the Git repository")

    report_parser = subparsers.add_parser("report", help="Generate a report from the log file")
    report_parser.add_argument("--event_type", type=str, help="Filter by event type (e.g., branch_creation, commit, push)")
    
    args = parser.parse_args()
    global REPO_PATH
    REPO_PATH = validate_repo_path(args.repo)

    if args.command == "monitor":
        threading.Thread(target=start_monitoring, args=(args.interval,), daemon=True).start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Exiting monitoring mode.")
    elif args.command == "report":
        generate_report(event_type=args.event_type)

if __name__ == "__main__":
    main()

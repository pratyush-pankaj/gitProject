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
REPO_PATH = os.getcwd()  # --repo तर्क के आधार पर सेट किया जाएगा
POLL_INTERVAL_DEFAULT = 5

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def run_git_command(args: List[str]) -> Optional[str]:
    """
    कॉन्फ़िगर किए गए REPO_PATH में Git कमांड चलाता है और इसके STDOUT को स्ट्रिंग के रूप में लौटाता है,
    या यदि कोई त्रुटि है तो None लौटाता है।
    """
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
    """
    'git branch --format=%(refname:short)' चलाकर रिपॉजिटरी में वर्तमान शाखाओं की सूची प्राप्त करता है।
    """
    output = run_git_command(["branch", "--format", "%(refname:short)"])
    if output:
        branches = [branch.strip() for branch in output.splitlines()]
        return branches
    return []

def get_latest_commit(branch: str) -> Optional[Dict]:
    """
    दी गई शाखा पर नवीनतम कमिट प्राप्त करता है। एक शब्दकोश लौटाता है:
    {
    "hash": <commit_hash>,

    "timestamp": <commit_unix_timestamp>,

    "message": <commit_message>

    }
    अगर कुछ विफल हो जाता है या शाखा में कोई कमिट नहीं है, तो None लौटाता है।
    """
    output = run_git_command(["log", branch, "-1", "--pretty=format:%H|%ct|%s"])
    if output:
        try:
            commit_hash, timestamp, message = output.split("|", 2)
            return {
                "hash": commit_hash,
                "timestamp": int(timestamp),
                "message": message
            }
        except ValueError:
            logging.error("Error parsing commit output.")
    return None

def log_event(event: Dict):
    """
    git_events.json में JSON-स्वरूपित ईवेंट जोड़ता है। स्पष्टता के लिए वर्तमान Unix टाइमस्टैम्प के साथ 'logged_at' फ़ील्ड जोड़ता है।
    """
    event["logged_at"] = int(time.time())
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(event) + "\n")
    except Exception as e:
        logging.error(f"Error writing to log file: {str(e)}")

async def monitor_repo(poll_interval: int = POLL_INTERVAL_DEFAULT):
    """
    शाखा निर्माण और नए कमिट के लिए Git रिपॉजिटरी की निगरानी करने के लिए एसिंक्रोनस फ़ंक्शन। यह हर 'poll_interval' सेकंड में पोल ​​करता है और इवेंट लॉग करता है।
    """
    logging.info("Starting repository monitoring...")
    
    # मौजूदा शाखाओं और उनके नवीनतम कमिट को ट्रैक करें
    known_branches = set(get_current_branches())
    branch_commits = {}
    
    # प्रत्येक ज्ञात शाखा के लिए नवीनतम कमिट आरंभ करें
    for branch in known_branches:
        commit = get_latest_commit(branch)
        if commit:
            branch_commits[branch] = commit["hash"]
    
    while True:
        try:
            current_branches = set(get_current_branches())
            
            # नई शाखाओं का पता लगाएं
            new_branches = current_branches - known_branches
            for branch in new_branches:
                event_data = {
                    "event_type": "branch_creation",
                    "branch": branch,
                    "timestamp": int(time.time())
                }
                log_event(event_data)
                logging.info(f"Detected new branch: {branch}")
                # नई शाखा के लिए प्रतिबद्धता आरंभ करें
                commit = get_latest_commit(branch)
                if commit:
                    branch_commits[branch] = commit["hash"]
            
            known_branches = current_branches
            
            # मौजूदा शाखाओं पर नए कमिट का पता लगाएं
            for branch in known_branches:
                commit = get_latest_commit(branch)
                if commit is None:
                    continue
                if branch not in branch_commits:
                    # यदि यह नई खोज है, तो केवल कमिट स्टोर करें, लेकिन लॉग न करें
                    branch_commits[branch] = commit["hash"]
                    continue
                
                # कमिट हैश की तुलना करें
                if commit["hash"] != branch_commits[branch]:
                    event_data = {
                        "event_type": "commit",
                        "branch": branch,
                        "commit_hash": commit["hash"],
                        "commit_message": commit["message"],
                        "commit_timestamp": commit["timestamp"]
                    }
                    log_event(event_data)
                    logging.info(f"New commit on {branch}: {commit['hash']}")
                    branch_commits[branch] = commit["hash"]
                    
        except Exception as e:
            logging.error(f"Error during monitoring: {str(e)}")
        
        await asyncio.sleep(poll_interval)

def start_monitoring(poll_interval: int):
    """
    मॉनिटर_रेपो को चलाने के लिए एक अलग थ्रेड में एक समर्पित asyncio इवेंट लूप शुरू करता है।
    यदि हम लूप को गैर-मुख्य थ्रेड में चलाना चाहते हैं तो यह पायथन 3.11+ पर आवश्यक है।
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(monitor_repo(poll_interval))
    except KeyboardInterrupt:
        logging.info("Monitoring stopped by user.")
    finally:
        loop.close()

def generate_report(
    developer: Optional[str] = None,
    event_type: Optional[str] = None,
    start_date: Optional[int] = None,
    end_date: Optional[int] = None
):
    """
    git_events.json फ़ाइल को पढ़ता है, दिए गए मापदंडों के अनुसार ईवेंट को फ़िल्टर करता है, और उन्हें मानव-पठनीय JSON प्रारूप में प्रिंट करता है।
    'डेवलपर' एक प्लेसहोल्डर है क्योंकि हम इस कोड में प्रतिबद्ध लेखकों को पार्स नहीं करते हैं।
    """
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
                
                # यदि निर्दिष्ट हो तो event_type द्वारा फ़िल्टर करें
                if event_type and event.get("event_type") != event_type:
                    continue
                
                # समय सीमा के अनुसार फ़िल्टर करें
                event_logged_at = event.get("logged_at", 0)
                if start_date and event_logged_at < start_date:
                    continue
                if end_date and event_logged_at > end_date:
                    continue
                
                # यदि आप तर्क को विस्तारित करना चाहते हैं तो डेवलपर के लिए प्लेसहोल्डर फ़िल्टर
                # उदाहरण के लिए, `git log` से कमिट लेखक को पुनः प्राप्त करके
                if developer:
                    # लागू नहीं किया गया है, लेकिन आप इसे प्रतिबद्ध डेटा से पार्स कर सकते हैं
                    pass
                
                events.append(event)
    except Exception as e:
        logging.error(f"Error reading log file: {str(e)}")
        return
    
    print(json.dumps(events, indent=2))

def validate_repo_path(path: str) -> str:
    """
    सत्यापित करता है कि 'पथ' एक वैध निर्देशिका है जिसमें .git फ़ोल्डर है।
    यदि वैध है तो पूर्ण पथ लौटाता है, अन्यथा स्क्रिप्ट से बाहर निकल जाता है।
    """
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
    
    # 'मॉनीटर' कमांड
    monitor_parser = subparsers.add_parser("monitor", help="Start monitoring the Git repository")
    monitor_parser.add_argument("--interval", type=int, default=POLL_INTERVAL_DEFAULT,
                                help="Polling interval in seconds")
    monitor_parser.add_argument("--repo", type=str, default=os.getcwd(),
                                help="Path to the Git repository (default: current directory)")
    
    # 'रिपोर्ट' कमांड
    report_parser = subparsers.add_parser("report", help="Generate a report from the log file")
    report_parser.add_argument("--event_type", type=str,
                               help="Filter by event type (e.g., branch_creation, commit)")
    report_parser.add_argument("--start_date", type=int,
                               help="Filter events logged after this Unix timestamp")
    report_parser.add_argument("--end_date", type=int,
                               help="Filter events logged before this Unix timestamp")
    report_parser.add_argument("--developer", type=str,
                               help="Filter by developer name (placeholder)")
    report_parser.add_argument("--repo", type=str, default=os.getcwd(),
                               help="Path to the Git repository (default: current directory)")
    
    args = parser.parse_args()
    
    # रेपो पथ को मान्य करें
    global REPO_PATH
    REPO_PATH = validate_repo_path(args.repo)
    
    if args.command == "monitor":
        # थ्रेड में पृष्ठभूमि निगरानी शुरू करें
        monitor_thread = threading.Thread(
            target=start_monitoring,
            args=(args.interval,),
            daemon=True
        )
        monitor_thread.start()
        
        # मुख्य थ्रेड को तब तक सक्रिय रखें जब तक उपयोगकर्ता Ctrl+C दबाता न रहे
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Exiting monitoring mode.")
            
    elif args.command == "report":
        generate_report(
            developer=args.developer,
            event_type=args.event_type,
            start_date=args.start_date,
            end_date=args.end_date
        )
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
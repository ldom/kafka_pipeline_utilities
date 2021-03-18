import argparse
import subprocess

from cli_utils import read_json_input

commands = {
    "recreates": "recreate_topics.py"
}

def main():
    parser = argparse.ArgumentParser(description="Reads a JSON file with commands to be executed")
    parser.add_argument("actions")
    parser.add_argument("cluster")

    args = parser.parse_args()
    json_commands = read_json_input(args.actions)['commands']

    for command in json_commands:
        if command in commands:
            kafka_utility_command = ["python", commands[command], args.actions, args.cluster]
            print(f'Running: {" ".join(kafka_utility_command)}')
            subprocess.run(kafka_utility_command)
        else:
            raise RuntimeError(f'Invalid Command ("{command}") Specified in "{args.actions}" -- Valid Commands: {", ".join(commands.keys())}')


if __name__ == "__main__":
    main()

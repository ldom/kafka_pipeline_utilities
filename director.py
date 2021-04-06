import argparse
import subprocess

from cli_utils import read_json_input


def get_brokers():
    return "list of brokers"  # kafka_broker.hosts


def main():
    parser = argparse.ArgumentParser(description="Reads a JSON file with commands to be executed")
    parser.add_argument("actions")
    args = parser.parse_args()

    brokers = get_brokers()

    commands = {
        "recreates": [ "python", "recreate_topics.py", args.actions, brokers ],
    }

    json_commands = read_json_input(args.actions)['commands']

    for command in json_commands:
        if command in commands:
            print(f'Running: { " ".join(commands[command]) }')
            subprocess.run(commands[command])
        else:
            raise RuntimeError(f'Invalid Command ("{command}") Specified in "{args.actions}" -- Valid Commands: {", ".join(commands.keys())}')


if __name__ == "__main__":
    main()
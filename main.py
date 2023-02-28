# /usr/bin/python3
import argparse
import logging
import pandas as pd
import shlex
import subprocess
import os
from typing import List
from s3 import add_s3_necesseary_args, upload_file
from cloudability import CloudabilityReport, Request, Filter, FilterOperator


def get_cloudability_token(args: argparse.Namespace) -> str:
    cloudaility_token = ""
    if args.token:
        cloudaility_token = args.token
    elif args.token_env_var:
        cloudaility_token = os.getenv(args.token_env_var)
    elif args.token_command:
        command = shlex.split(args.token_command)
        cloudaility_token = subprocess.check_output(
            command).decode("utf-8").replace("\n", "")
    else:
        raise RuntimeError("no cloudability token was provided")
    if not cloudaility_token:
        raise RuntimeError("Not cloudability token provided")
    if not isinstance(cloudaility_token, str):
        raise RuntimeError("invalid cloudability token provided")
    return cloudaility_token


def setup_logging(args: argparse.Namespace) -> None:
    level = logging.INFO
    if args.log_level == "DEBUG":
        level = logging.DEBUG
    if args.log_level == "WARN":
        level = logging.WARN
    if args.log_level == "ERROR":
        level = logging.ERROR
    logging.basicConfig(
        format='%(asctime)s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=level)


def pr_costs(cloudability: CloudabilityReport, days=7) -> pd.DataFrame:
    def group_by_and_sum(df: pd.DataFrame, fields: List[str]) -> pd.DataFrame:
        """"
        Utility function to group by a panda data frame on a couple of keys and to
        aggregate the other columns with a sum. Columns that can't be summed will
        be removed from the resulting data frame.
        """
        ret = df.groupby(
            fields
        ).sum(
            numeric_only=True
        )
        if isinstance(ret, pd.DataFrame):
            return ret
        raise RuntimeError("Invalid type")

    def name_from_label(a: str) -> str:
        return cloudability.measures.name_from_label(a)

    # get the total cost report
    total_cost = pd.DataFrame(
        cloudability.run_request(
            Request(
                filters=[
                    Filter(
                        name_from_label("Account ID"),
                        FilterOperator.EQUALS,
                        "933752197999"),
                    Filter(
                        name_from_label("Workload (value)"),
                        FilterOperator.EQUALS,
                        "ci runner")
                ],
                dimensions=[
                    name_from_label("Project (value)"),
                    name_from_label("Business Unit (value)"),
                ],
                metrics=[
                    name_from_label("Cost (Total)"),
                    name_from_label("Usage Hours")
                ],
                mappings={
                    name_from_label("Project (value)"): "Project",
                    name_from_label("Business Unit (value)"): "Branch",
                    name_from_label("Cost (Total)"): "Cost",
                    name_from_label("Usage Hours"): "Usage_Hours"
                },
                days=days
            )
        )
    )

    # get the rogue cost report
    rogue_cost = pd.DataFrame(
        cloudability.run_request(
            Request(
                filters=[
                    Filter(
                        name_from_label("Account ID"),
                        FilterOperator.EQUALS,
                        "933752197999"),
                    Filter(
                        name_from_label("Usage Hours"),
                        FilterOperator.GREATER_THAN_OR_EQUALS,
                        "4"),
                    Filter(
                        name_from_label("Workload (value)"),
                        FilterOperator.EQUALS,
                        "ci runner")
                ],
                dimensions=[
                    name_from_label("Project (value)"),
                    name_from_label("Business Unit (value)"),
                    name_from_label("Resource ID")
                ],
                metrics=[
                    name_from_label("Cost (Total)"),
                    name_from_label("Usage Hours")
                ],
                mappings={
                    name_from_label("Project (value)"): "Project",
                    name_from_label("Business Unit (value)"): "Branch",
                    name_from_label("Cost (Total)"): "Rogue_Cost",
                    name_from_label("Resource ID"): "Resource_ID",
                    name_from_label("Usage Hours"): "Rogue_Usage_Hours"
                },
                days=days
            )
        )
    )

    # Rogue cost is the cost per hour if the total time is above 4h, which is
    # not really accurate. The real cost is everything above 4h of time. So we
    # need to adjust for this.
    # (Rogue_Cost / Rogue_Usage_Hours) * 4 is the cost we're willing to pay and
    # should not be considered lost.
    #
    # Rogue_Cost = Rogue_Cost - ((Rogue_Cost / Rogue_Usage_Hours)*4)
    #
    # This has to be computed before any sums because it's 4h per machine.
    rogue_cost["Rogue_Cost"] = rogue_cost["Rogue_Cost"] - (
        (rogue_cost["Rogue_Cost"] / rogue_cost["Rogue_Usage_Hours"]) * 4
    )
    rogue_cost["Rogue_Usage_Hours"] = rogue_cost["Rogue_Usage_Hours"] - 4

    # Join the two costs, they've got to be grouped by on the same keys and
    # summed up so that there's only one line per key.
    costs = pd.concat(
        [
            group_by_and_sum(total_cost, ["Project", "Branch"]),
            group_by_and_sum(rogue_cost, ["Project", "Branch"])
        ],
        axis=1
    )

    # Set NaN values to 0 (only Rogue cost can be to NaN since Cost is the
    # total cost)
    costs[
        ["Rogue_Usage_Hours", "Rogue_Cost"]
    ] = costs[
        ["Rogue_Usage_Hours", "Rogue_Cost"]
    ].fillna(0)

    # Then adjust the Cost value to be Cost - Rogue_Cost (same for hours)
    # This way it's more clear what is wanted cost and lost cost in the tables
    # and we don't have too much redundant information
    costs["Cost"] = costs["Cost"] - costs["Rogue_Cost"]
    costs["Usage_Hours"] = costs["Usage_Hours"] - costs["Rogue_Usage_Hours"]
    costs = costs.sort_values(by=['Cost'])
    return costs


def main():
    parser = argparse.ArgumentParser(description='Get data from cloudability')
    parser.add_argument("--log-level",
                        type=str,
                        choices=[
                            "DEBUG",
                            "INFO",
                            "WARN",
                            "ERROR",
                        ],
                        default="INFO",
                        help="The log level"
                        )
    # cloudability args
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--token",
                       type=str,
                       help="cloudability token"
                       )
    group.add_argument("--token-env-var",
                       type=str,
                       help="env var for cloudability token"
                       )
    group.add_argument("--token-command",
                       default="pass cloudability_secret",
                       type=str,
                       help="Command to retrieve the cloudability token"
                       )
    parser.add_argument("--days",
                        type=int,
                        default=7,
                        help="How many days to retrieve"
                        )
    # s3 args
    add_s3_necesseary_args(parser)
    # commands args
    subparsers = parser.add_subparsers(title="command",
                                       required=True,
                                       dest='command',
                                       help='Command to execute')
    subparsers.add_parser("print", help="Write result on stdout")
    # export to csv
    csv_parser = subparsers.add_parser("csv", help="Export as CSV")
    csv_parser.add_argument("--filename",
                            required=True,
                            type=str,
                            help="The output file"
                            )
    # export to parquet
    parquet_parser = subparsers.add_parser(
        "parquet", help="Export as parquet files")
    parquet_parser.add_argument("--filename",
                                required=True,
                                type=str,
                                help="The output file"
                                )
    parquet_parser.add_argument("--engine",
                                type=str,
                                choices=[
                                    "pyarrow",
                                    "fastparquet",
                                ],
                                default="pyarrow",
                                help="The parquet engine"
                                )
    parquet_parser.add_argument("--compression",
                                type=str,
                                choices=[
                                    "snappy",
                                    "gzip",
                                    "brotli"
                                ],
                                default=None,
                                help="The output compression, default None"
                                )

    args = parser.parse_args()

    setup_logging(args)

    # Gather the data
    prc = pr_costs(
        CloudabilityReport(
            get_cloudability_token(args)
        ),
        args.days
    )

    # Apply the command
    if args.command == "csv":
        prc.to_csv(
            args.filename,
            encoding='utf-8'
        )
    elif args.command == "parquet":
        prc.to_parquet(
            args.filename,
            compression=args.compression
        )
    else:
        print(prc)

    # Upload to s3 if necesseary
    if args.command != "print":
        upload_file(args.filename, args)


if __name__ == '__main__':
    main()

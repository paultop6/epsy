import click
import boto3
import botocore
import json
import jwt
from io import BytesIO
from gql import gql as gql_generator, Client
from gql.transport.aiohttp import AIOHTTPTransport
import pandas as pd
import gql.transport.exceptions as gql_exceptions
import logging
import sys
from typing import Tuple, Dict, Any, Union


logger = logging.getLogger("event_task")
logger.addHandler(logging.StreamHandler(sys.stdout))


def get_credentials(user: str, password: str, client_id: str, pool_id: str) -> Tuple:
    """
    Communicates with Cognito to get temporary credentials
    for use with appsync api and s3.
    """
    session = boto3.Session(region_name="us-east-1")

    client_idp = session.client(
        "cognito-idp", config=boto3.session.Config(signature_version=botocore.UNSIGNED)
    )
    client_identity = session.client(
        "cognito-identity",
        config=boto3.session.Config(signature_version=botocore.UNSIGNED),
    )

    try:
        auth = client_idp.initiate_auth(
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={"USERNAME": user, "PASSWORD": password},
            ClientId=client_id,
        )

        try:
            decoded_data = jwt.decode(
                jwt=auth["AuthenticationResult"]["AccessToken"],
                algorithms=["RS256"],
                options={"verify_signature": False},
            )
        except jwt.exceptions.PyJWTError as e:
            logger.error(f"Failed to process jwt {str(e)}")
            return None, None
    except botocore.exceptions.ClientError as e:
        logger.error(f"cognito-idp call failure {str(e)}")
        return None, None

    if "iss" in decoded_data.keys():
        if decoded_data["iss"].startswith("https://"):
            iss = decoded_data["iss"][8:]
        else:
            iss = decoded_data["iss"]
    else:
        logger.error(f"iss missing from decoded jwt")
        return None, None

    try:
        identity = client_identity.get_id(
            IdentityPoolId=pool_id,
            Logins={iss: auth["AuthenticationResult"]["IdToken"]},
        )

        credentials = client_identity.get_credentials_for_identity(
            IdentityId=identity["IdentityId"],
            Logins={iss: auth["AuthenticationResult"]["IdToken"]},
        )
    except botocore.exceptions.ClientError as e:
        logger.error(f"cognito-identity call failure {str(e)}")
        return None, None

    return credentials, auth


def fetch_csv_from_s3(
    aws_creds: Dict[Any, Any], bucket_path: str
) -> Union[BytesIO, None]:
    """
    Fetch events csv file from s3 bucket, returning a BytesIO object on success, None on failure
    """
    cred_session = boto3.session.Session(
        aws_access_key_id=aws_creds["Credentials"]["AccessKeyId"],
        aws_secret_access_key=aws_creds["Credentials"]["SecretKey"],
        aws_session_token=aws_creds["Credentials"]["SessionToken"],
    )

    client_s3 = cred_session.client("s3")

    f_obj = BytesIO()

    logger.debug("Downloading object from s3...")
    try:
        client_s3.download_fileobj(*bucket_path.split("/"), f_obj)
        f_obj.seek(0)
    except botocore.exceptions.ClientError as e:
        logger.error(f"s3 download_fileobj failure {str(e)}")
        return None

    return f_obj


def get_user(user_id: str, api: str, auth_token: str) -> Union[Dict, None]:
    """
    Creates and dispatches a GrapqhQL request to fetch User details
    for the given user_id.  Return response payload in success, None on failure.
    """

    transport = AIOHTTPTransport(
        url=api,
        headers={
            "Authorization": auth_token,
        },
    )
    gql_client = Client(
        transport=transport, fetch_schema_from_transport=True, execute_timeout=30
    )

    query = gql_generator(
        """
        query GetUser($userId: String!) {
            fetchUser(id: $userId) {
                id
                firstName
                lastName
                role
            }
        }
        """
    )

    params = {"userId": user_id}

    try:
        result = gql_client.execute(query, variable_values=params)
    except gql_exceptions.TransportQueryError as e:
        error_message = []
        for error in e.errors:
            if isinstance(error, dict):
                error_message.append(error["message"])
        logger.warning(f"Server Query failed, reason(s): {', '.join(error_message)}")
        result = None

    return result


def get_daily_stats(
    user: str, password: str, client_id: str, pool_id: str, api: str, bucket_path: str
) -> Union[Dict, None]:
    """
    Fetches raw stats data from s3 bucket, and uses pandas dataframes to
    generate statistics for all event data.  Also fetches user data from GraphQL API
    to associate with id found in event data.  Returns stats dict on success, None on failure.
    """
    logger.debug("Get Cognito Credentials")
    aws_creds, auth_token = get_credentials(user, password, client_id, pool_id)

    if aws_creds is None or auth_token is None:
        logger.error(f"Failed to get credentials from Cognito")
        return None

    logger.debug(f"Fetch event data from s3 path {bucket_path}")
    csv_data = fetch_csv_from_s3(aws_creds, bucket_path)

    if csv_data is None:
        logger.error(f"Failed to get csv data from s3")
        return None

    df = pd.read_csv(csv_data)

    # Add role column for use later when merging role information for users.
    df["role"] = None

    all_user_info = []
    stats = {}
    failed_user_ids = []
    events_per_day = len(df.index)

    uniq_users = df["user"].drop_duplicates()
    num_uniq_users = len(uniq_users.index)

    logger.debug(f"Processing {num_uniq_users} unique users")

    for i, user in enumerate(uniq_users):
        logger.debug(f"{i+1}/{num_uniq_users}: {user}")

        # Get User info from GraphQL API.
        user_info = get_user(
            user, api, auth_token["AuthenticationResult"]["AccessToken"]
        )
        if user_info is None:
            logger.warning(f"Failed to fetch user info for id {user}, ignoring.")
            failed_user_ids.append(user)
            continue

        # Update role column for records in dataframe for the given user id.
        df.loc[df["user"] == user, "role"] = user_info["fetchUser"]["role"]
        all_user_info.append(user_info["fetchUser"])

    df_user_info = pd.DataFrame(
        all_user_info, columns=["id", "firstName", "lastName", "role"]
    )

    # Get number of unique roles
    role_counts = df["role"].value_counts()

    # Get top 10 for number of events per user.
    user_counts = df["user"].value_counts().nlargest(10)
    df_user_counts = pd.DataFrame(user_counts)
    df_user_counts = df_user_counts.reset_index()

    # Change column names for following merge.
    df_user_counts.columns = ["user", "counts"]

    # Merge two dict together on user/id columns.  This gives a final list
    # of records that contain all relevant user/event data.
    df_merged = df_user_counts.merge(df_user_info, left_on="user", right_on="id")

    # Drop user column as the data is a duplicate of the id column.
    df_merged = df_merged.drop(["user"], axis=1)

    # Convert all NaNs to None for better python processing.
    df_merged = df_merged.where(df_merged.notnull(), None)

    stats = {
        "events_per_day": events_per_day,
        "role_counts": role_counts.to_dict(),
        "top_10_users": df_merged.to_dict(orient="records"),
    }

    output = {
        "statistics": stats,
        "user_info": all_user_info,
        "failed_users": failed_user_ids,
    }

    return output


@click.command()
@click.option(
    "--user",
    prompt="Username for Cognito Authentication",
    help="Username for Cognito Authentication",
)
@click.option(
    "--password",
    prompt="Password for Cognito Authentication",
    hide_input=True,
    help="Password for Cognito Authentication",
)
@click.option(
    "--client_id",
    prompt="Client Id for Cognito Authentication",
    help="Client Id for Cognito Authentication.",
)
@click.option(
    "--pool_id",
    prompt="Identity Pool Id for Cofnito Authentication",
    help="Identity Pool Id for Cofnito Authentication.",
)
@click.option("--api", prompt="GraphQl API Endpoint", help="GraphQl API Endpoint.")
@click.option(
    "--bucket_path",
    prompt="Bucket name and path to file (<bucket_name>/file_path)",
    help="Bucket name and path to file (<bucket_name>/file_path).",
)
@click.option("--debug", default=False, is_flag=True, help="Enable verbose logging.")
@click.option(
    "--to_file",
    default=False,
    is_flag=True,
    help="Store output json to file output.json",
)
def event_tracking(
    user, password, client_id, pool_id, api, bucket_path, debug, to_file
):
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    stats = get_daily_stats(user, password, client_id, pool_id, api, bucket_path)

    if stats is not None:
        logger.info(json.dumps(stats, indent=4))
        if to_file:
            with open("output.json", "w") as f:
                json.dump(stats, f, indent=4)
        sys.exit(0)
    else:
        logger.error(f"Failed to get stats, exiting")
        sys.exit(1)


if __name__ == "__main__":
    event_tracking()

# Improvements
# - Allow username and password to be saved in .env file so as to stop manual input.
#   Using click.options `hide_input` works when entering from the prompt.  But using
#   `default` means the password is leaked to stdout which is poor security practice.
#   This is a bug/limitation of click.
# - Encapsulate main functionality in a portable utility class that could be used by
#   other programs.
# - Depending on target system (linux, windows, mac), a portage package could be made
#   that would include all dependencies for the program.

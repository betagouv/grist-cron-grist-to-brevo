import os
import sys
from datetime import date

import psycopg
from psycopg.rows import dict_row
import requests

ID_BREVO_LIST = [int(id) for id in os.environ["ID_BREVO_LIST"].split(",")]
ATTRS_PREFIX = os.environ.get("BREVO_ATTRS_PREFIX", "")
MAX_BATCH_SIZE = 5000

brevo_url = "https://api.brevo.com/v3/contacts/import"

brevo_headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "api-key": os.environ["BREVO_API_KEY"],
}

error_counter = 0

with psycopg.connect(conninfo=os.environ["PG_URL"]) as conn:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            WITH raw_brevo_data AS (
                SELECT 
                    logins.display_email AS EMAIL,
                    INITCAP(SPLIT_PART(users.name, '.', 1)) AS PRENOM,
                    INITCAP(SPLIT_PART(users.name, '.', 2)) AS NOM,
                    SPLIT_PART(EMAIL, '@', 2) AS DOMAIN,
                    users.first_login_at AS FIRST_LOGIN,
                    users.last_connection_at AS LAST_LOGIN,
                    users.last_connection_at::date - users.first_login_at::date AS NB_DAYS_BETWEEN_FIRST_AND_LAST_LOGIN,
                    CURRENT_DATE::date - users.last_connection_at::date AS INACTIVITY,
                    users.options -> 'ssoExtraInfo' ->> 'siret' AS SIRET,
                    users.type AS TYPE,
                    docs.id AS DOC_ID
                FROM 
                    users LEFT JOIN
                    logins ON users.id = logins.user_id LEFT JOIN
                    docs ON logins.user_id = docs.created_by
            )
            SELECT 
                EMAIL,
                PRENOM,
                NOM,
                DOMAIN,
                FIRST_LOGIN,
                LAST_LOGIN,
                NB_DAYS_BETWEEN_FIRST_AND_LAST_LOGIN,
                INACTIVITY,
                SIRET,
                TYPE,
                count(DOC_ID) AS NB_DOCUMENTS
            FROM raw_brevo_data
            GROUP BY
                EMAIL,
                PRENOM,
                NOM,
                DOMAIN,
                FIRST_LOGIN,
                LAST_LOGIN,
                NB_DAYS_BETWEEN_FIRST_AND_LAST_LOGIN,
                INACTIVITY,
                SIRET,
                TYPE;
            """
        )
        users = cur.fetchall()

brevo_payload = {
    "emailBlacklist": False,
    "disableNotification": False,
    "smsBlacklist": False,
    "updateExistingContacts": True,
    "emptyContactsAttributes": False,
    "jsonBody": [],
    "listIds": ID_BREVO_LIST,
}


def user_to_brevo_attributes(user):
    return {
        "PRENOM": user["prenom"],
        "NOM": user["nom"],
        "USER_DOMAIN": user["domain"],
        ATTRS_PREFIX + "USER_FIRST_LOGIN": normalize_date(user["first_login"]),
        ATTRS_PREFIX + "USER_LAST_LOGIN": normalize_date(user["last_login"]),
        ATTRS_PREFIX
        + "USER_NB_DAYS_BETWEEN_FIRST_AND_LAST_LOGIN": user[
            "nb_days_between_first_and_last_login"
        ],
        ATTRS_PREFIX + "USER_INACTIVITY": user["inactivity"],
        "USER_SIRET": user["siret"],
        ATTRS_PREFIX + "USER_NB_DOCUMENTS": user["nb_documents"],
    }


def normalize_date(value: date | None) -> str | None:
    return value.strftime("%Y-%m-%d") if value is not None else value


def prepare_payload(users):
    for user in users:
        brevo_payload["jsonBody"].append(
            {"email": user["email"], "attributes": user_to_brevo_attributes(user)}
        )


sanitized_users = [user for user in users if user["type"] == "login"]
chunked_users = [
    sanitized_users[start : start + MAX_BATCH_SIZE]
    for start in range(0, len(sanitized_users), MAX_BATCH_SIZE)
]

for chunk in chunked_users:
    brevo_payload["jsonBody"] = []
    prepare_payload(chunk)
    response = requests.post(brevo_url, json=brevo_payload, headers=brevo_headers)
    print(response.text)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        print(f"Status code {response.status_code} sending chunk of users")
        error_counter += 1

sys.exit(1 if error_counter > 0 else 0)

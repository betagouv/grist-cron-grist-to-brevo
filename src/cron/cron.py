import os
import sys
from datetime import date

import psycopg
import requests

ID_BREVO_LIST = [int(id) for id in os.environ["ID_BREVO_LIST"].split(',')]
ATTRS_PREFIX = os.environ.get("BREVO_ATTRS_PREFIX", "")
MAX_BATCH_SIZE = 5000

brevo_url = "https://api.brevo.com/v3/contacts/import"

brevo_headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "api-key": os.environ["BREVO_API_KEY"]
}

error_counter = 0

with psycopg.connect(conninfo = os.environ["PG_URL"]) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            WITH raw_brevo_data AS (
                SELECT 
                    logins.display_email AS EMAIL,
                    INITCAP(SPLIT_PART(users.name, '.', 1)) AS PRENOM,
                    INITCAP(SPLIT_PART(users.name, '.', 2)) AS NOM,
                    SPLIT_PART(EMAIL, '@', 2) AS USER_DOMAIN,
                    users.first_login_at AS USER_FIRST_LOGIN,
                    users.last_connection_at AS USER_LAST_LOGIN,
                    users.last_connection_at::date - users.first_login_at::date AS USER_NB_DAYS_BETWEEN_FIRST_AND_LAST_LOGIN,
                    CURRENT_DATE::date - users.last_connection_at::date AS USER_INACTIVITY,
                    users.options -> 'ssoExtraInfo' ->> 'siret' AS USER_SIRET,
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
                USER_DOMAIN,
                USER_FIRST_LOGIN,
                USER_LAST_LOGIN,
                USER_NB_DAYS_BETWEEN_FIRST_AND_LAST_LOGIN,
                USER_INACTIVITY,
                USER_SIRET,
                count(DOC_ID) AS USER_NB_DOCUMENTS
            FROM raw_brevo_data
            GROUP BY
                EMAIL,
                PRENOM,
                NOM,
                USER_DOMAIN,
                USER_FIRST_LOGIN,
                USER_LAST_LOGIN,
                USER_NB_DAYS_BETWEEN_FIRST_AND_LAST_LOGIN,
                USER_INACTIVITY,
                USER_SIRET;
            """)
        users = cur.fetchall()

brevo_payload = {
                    "emailBlacklist": False,
                    "disableNotification": False,
                    "smsBlacklist": False,
                    "updateExistingContacts": True,
                    "emptyContactsAttributes": False,
                    "jsonBody":[],
                    "listIds": ID_BREVO_LIST,
                }

brevo_attributes = [
                    "PRENOM",
                    "NOM",
                    "USER_DOMAIN",
                    ATTRS_PREFIX + "USER_FIRST_LOGIN",
                    ATTRS_PREFIX + "USER_LAST_LOGIN",
                    ATTRS_PREFIX + "USER_NB_DAYS_BETWEEN_FIRST_AND_LAST_LOGIN",
                    ATTRS_PREFIX + "USER_INACTIVITY",
                    "USER_SIRET",
                    ATTRS_PREFIX + "USER_NB_DOCUMENTS"
                    ]

EMAIL_ATTR_INDEX = 0
USER_FIRST_LOGIN_INDEX = 3
USER_LAST_LOGIN_INDEX = 4

def normalize_date(value: date|None) -> str|None:
    return value.strftime('%Y-%m-%d') if value is not None else value

def prepare_payload(users):
    for user in users:
        user = list(user)
        email = user.pop(EMAIL_ATTR_INDEX)
        user[USER_FIRST_LOGIN_INDEX] = normalize_date(user[USER_FIRST_LOGIN_INDEX])
        user[USER_LAST_LOGIN_INDEX] = normalize_date(user[USER_LAST_LOGIN_INDEX])
        brevo_payload["jsonBody"].append(
            {
                "email": email,
                "attributes" : dict(zip(brevo_attributes, user))
            }
        )

chunked_users = [users[start:start+MAX_BATCH_SIZE] for start in range(0,len(users),MAX_BATCH_SIZE)]

for chunk in chunked_users:
    brevo_payload["jsonBody"] = []
    prepare_payload(chunk)
    response = requests.post(brevo_url, json=brevo_payload, headers=brevo_headers)
    print(response.text)
    if response.status_code != 200:
        error_counter += 1

sys.exit(error_counter)

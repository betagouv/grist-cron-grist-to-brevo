import os
from datetime import date

import psycopg
from psycopg.rows import dict_row
import requests

# Base lists we want the user inscribed to.
ID_BREVO_LIST = [int(id) for id in os.environ["ID_BREVO_LIST"].split(',')]
# Event lists that the user can unsuscribe from
# And we want to enroll him/her only once.
ID_BREVO_OPTIONAL_LIST = [int(id) for id in os.environ["ID_BREVO_OPTIONNAL_LIST"].split(',')]
ATTRS_PREFIX = os.environ.get("BREVO_ATTRS_PREFIX", "")

brevo_url = "https://api.brevo.com/v3/contacts/import"

brevo_headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "api-key": os.environ["BREVO_API_KEY"]
}

with psycopg.connect(conninfo = os.environ["PG_URL"]) as conn:
    with conn.cursor(row_factory=dict_row) as cur:
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

brevo_new_users_payload = brevo_payload.copy()
brevo_new_users_payload["listIds"] = ID_BREVO_OPTIONAL_LIST

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

def normalize_date(value: date|None) -> str|None:
    return value.strftime('%Y-%m-%d') if value is not None else value

for user in users:
    print(user)
    email = user["email"]
    user.pop("email")
    user["user_first_login"] = normalize_date(user["user_first_login"])
    user["user_last_login"] = normalize_date(user["user_last_login"])
    # We enroll users only when they have just been registered to the instance
    if user["user_nb_days_between_first_and_last_login"] == 0:
        brevo_new_users_payload["jsonBody"].append(
            {
                "email": email,
                "attributes" : dict(zip(brevo_attributes, user.values()))
            }
    )
    brevo_payload["jsonBody"].append(
        {
            "email": email,
            "attributes" : dict(zip(brevo_attributes, user.values()))
        }
    )

# Update or create users in brevo
# Add them to default "technical" lists which they can't unsubscribe
# TODO remove them from BREVO when they are soft deleted from Grist
response = requests.post(brevo_url, json=brevo_payload, headers=brevo_headers)
print(response.text)
# Add new users to optional lists they can unsubscribe later
response_new_users = requests.post(brevo_url, json=brevo_new_users_payload, headers=brevo_headers)
print(response_new_users.text)

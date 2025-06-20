import os
from urllib.parse import urlparse

import psycopg
import requests

ID_BREVO_LIST = int(os.environ["ID_BREVO_LIST"])

brevo_url = "https://api.brevo.com/v3/contacts/import"

brevo_headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "api-key": os.environ["BREVO_API_KEY"]
}

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
                    users.last_connection_at::date - users.first_login_at::date AS USER_NB_DAYS_BETWEEN_FIRST_AND_LAST_CONNECTION,
                    CURRENT_DATE::date - users.last_connection_at::date AS USER_INACTIVITY,
                    (users.options -> 'ssoExtraInfo' -> 'siret')::text AS USER_SIRET,
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
                USER_NB_DAYS_BETWEEN_FIRST_AND_LAST_CONNECTION,
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
                USER_NB_DAYS_BETWEEN_FIRST_AND_LAST_CONNECTION,
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
                    "listIds": [ ID_BREVO_LIST ]
                }

brevo_attributes = [
                    "PRENOM",
                    "NOM",
                    "USER_DOMAIN",
                    "USER_FIRST_LOGIN",
                    "USER_LAST_LOGIN",
                    "USER_NB_DAYS_BETWEEN_FIRST_AND_LAST_CONNECTION",
                    "USER_INACTIVITY",
                    "USER_SIRET",
                    "USER_NB_DOCUMENTS"
                    ]

for user in users:
    user = list(user)
    email = user.pop(0)
    user[3] = user[3].strftime('%Y-%m-%d') if user[3] is not None else user[3]
    user[4] = user[4].strftime('%Y-%m-%d') if user[4] is not None else user[3]
    brevo_payload["jsonBody"].append(
        {
            "email": email,
            "attributes" : dict(zip(brevo_attributes, user))
        }
    )

response = requests.post(brevo_url, json=brevo_payload, headers=brevo_headers)
print(response.text)

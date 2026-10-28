# dbt setup guide (G9 AIS & Port Data)

How to get the dbt project running on your machine. It takes about 15 minutes.

## What is already done

- Snowflake database `AIS` with schemas `RAW`, `STAGING`, `MARTS`
- Shared role `DBT_ROLE` and warehouse `AIS_WH` (XSMALL, auto-suspends after 60 seconds)
- `RAW.PORTS_ACTIVITY` (about 5.9M rows) and `RAW.PORTS_REF` (2,065 rows), loaded from the S3 silver layer
- A dbt project in the `dbt/` folder with sources, two staging models and tests

You only need to connect your own machine and your own Snowflake user.

## 1. Prerequisites

- Python 3.11 or 3.12
- git
- A Snowflake user with `DBT_ROLE` granted (GRANT ROLE DBT_ROLE TO USER <snowflake_username>;)

## 2. Get the code

```bash
git pull
cd dbt/ais          # the folder that contains dbt_project.yml
```

## 3. Install dbt

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install dbt
dbt --version
```

If the repo has a `requirements.txt`, use `pip install -r requirements.txt` instead so your versions match everyone else's.

## 4. Create your own key pair

Authentication uses a key pair. Every person has their own key, and nothing is shared. Never use anyone else's key.

```bash
mkdir -p ~/.snowflake_keys && chmod 700 ~/.snowflake_keys
openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 aes256 -inform PEM -out ~/.snowflake_keys/dbt_key.p8
# it asks for a passphrase: choose one and remember it
openssl rsa -in ~/.snowflake_keys/dbt_key.p8 -pubout -out ~/.snowflake_keys/dbt_key.pub
grep -v "KEY" ~/.snowflake_keys/dbt_key.pub | tr -d '\n'
```

The last command prints your **public** key on one line. That one is safe to share. The `.p8` file and the passphrase are private and never leave your machine.

## 5. Attach the public key to your Snowflake user

Run this in Snowsight with your own user, or send the public key line to Hady and he runs it:

```sql
ALTER USER <YOUR_SNOWFLAKE_USERNAME> SET RSA_PUBLIC_KEY='<paste the single line>';
```

## 6. Set up your profile

Create `~/.dbt/profiles.yml` (the `~/.dbt` folder may not exist yet). Hady's file works as a base. Change **only** the four marked lines.

```yaml
ais:
  target: dev
  outputs:
    dev:
      type: snowflake
      threads: 16
      account: OBTNPMW-PW53886
      user: HADYELFADALY #change this to your own Snowflake username
      database: AIS
      warehouse: AIS_WH
      schema: RAW
      role: DBT_ROLE
      private_key_path: /home/hady/Hady/CS/Data Engineering/SIC/Graduation Project/keys/rsa_key.p8 #change this to your own path
```

Do not change `account`, `role`, `warehouse`, `database` or `threads`.

## 7. Check the connection and build

```bash
dbt debug       # must end with "All checks passed"
dbt build       # builds the models and runs the tests
```

If both pass, you are ready to work.

## Working rules

- **Never commit** `profiles.yml`, `*.p8` or `*.pub` files, or anything containing a passphrase.
- **Work in your own schema** (`DEV_<YOURNAME>`). The shared schemas `STAGING` and `MARTS` are for official builds only.
- **Don't modify the `RAW` tables.** They are loaded from S3 by a controlled process. If a column or table is missing in RAW, ask Hady.
- Work on a git branch and open a pull request, instead of pushing straight to `main`.
- The warehouse suspends itself after 60 seconds idle. Don't change its size without telling the team, because trial credits are shared.

## Troubleshooting

| Problem | Likely cause |
|---|---|
| `JWT token is invalid` | The public key attached to your user doesn't match your private key. Redo steps 4 and 5, and check that you pasted a single line without the header and footer lines. |
| `Could not deserialize key data` | Wrong passphrase, or `SNOWFLAKE_KEY_PASSPHRASE` isn't set in this terminal. |
| `Failed to connect` or an account error | The `account` value should look like `orgname-accountname`. |
| `Role 'DBT_ROLE' ... not granted` | Ask Hady to run `GRANT ROLE DBT_ROLE TO USER <you>;`. |
| `Profile g9_ais not found` | The profile name in `profiles.yml` must match `profile:` in `dbt_project.yml`. |
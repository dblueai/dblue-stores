from decouple import config

CREDENTIALS_AUTH_MOUNT_PATH = config("CREDENTIALS_AUTH_MOUNT_PATH", default="/.dblue/credentials")

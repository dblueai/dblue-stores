from decouple import config

DATASET_AUTH_MOUNT_PATH = config("DATASET_AUTH_MOUNT_PATH", default="/.dblue/credentials")

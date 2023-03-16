def get_config():
    return {
        "s3.amazonaws.com": {
            "aws_secret_access_key": "aws_key",
            "aws_access_key_id": "secret_key",
            "is_secure": True
        },
        "s3.myinstallation.org": {
            "aws_secret_access_key": "my_key",
            "aws_access_key_id": "my_secret_key",
            "is_secure": False
        },
    }

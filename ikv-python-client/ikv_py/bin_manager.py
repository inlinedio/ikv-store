import os, platform
import boto3
import re

from typing import Optional, Tuple

from utils import is_valid_str_or_raise, compare_semver

REGION = "us-west-2"
BUCKET_NAME = "ikv-binaries"
SEMVER_PATTERN = r'^(\d+)\.(\d+)\.(\d+)(?:-([\w\.]+))?(?:\+([\w\.]+))?$'
CHUNK_SIZE = 8388608 # 8KB 

class NativeBinaryManager:
    """
    Responsible for dowloading native IKV binary, based on the host
    platform, for dynamic loading.
    """
    def __init__(self, mount_dir: str):
        self.s3_client = boto3.client('s3', region_name=REGION)
        self.mount_dir = is_valid_str_or_raise(mount_dir)
    
    def _init_mount_dir(self):
        """
        Ensures mount & mount/bin directories are usable.
        """
        # creates parent directories if required
        os.makedirs("{}/bin".format(self.mount_dir), exist_ok=True)

    def get_path_to_dll(self) -> Optional[str]:
        self._init_mount_dir()

        # ex. 0.0.3, /path/to/mount/bin/0.0.3-libikv.so
        local_semver, local_dll_path = self._local_dll_details()
        
        # ex. 0.0.5, release/mac-aarch64/0.0.5-libikv.dylib
        remote_semver, remote_dllfile_key = self._remote_dll_details()

        # nothing available on remote - return Optional local
        if remote_semver is None or remote_dllfile_key is None:
            return local_dll_path
        
        # local is present and is higher or equal as compared to remote
        if local_semver is not None and compare_semver(remote_semver, local_semver) <= 0:
            return local_dll_path
        
        # download remote
        return self._download_remote_dll(remote_dllfile_key)

    def _local_dll_details(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Local dll details if it exists (or None, None).
        Return: semver, path tuple, ex. (0.0.3, ..mount-directory/bin/0.0.3-libikv.so)
        """
        path = "{}/bin".format(self.mount_dir)
        filenames = os.listdir(path=path)
        if not filenames: 
            return None, None
        
        for filename in filenames:
            # filename: 0.0.3-libikv.so
            semver = NativeBinaryManager.parse_semver(filename)
            if semver is not None:
                return semver, "{}/bin/{}".format(self.mount_dir, filename)
            
        return None, None
    
    def _remote_dll_details(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Remote dll details (highest available semver) if it exists (or None, None).
        Return: semver, filename tuple, ex. (0.0.5, release/mac-aarch64/0.0.5-libikv.dylib)
        """
        # release/{mac|linux|windows}-{aarch64|x86_64|tbd}
        platform_prefix = NativeBinaryManager.platform_s3_prefix()
        if platform_prefix is None:
            return None, None

        response = self.s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=platform_prefix)
        
        remote_files = []
        if 'Contents' in response:
            objects = [obj for obj in response['Contents'] if obj is not None]
            keys = [obj['Key'] for obj in objects if obj['Key'] is not None]
            remote_files = [key.split("/")[-1] for key in keys] # output: ["foo", "0.0.3-libikv.so", "bar"]
            remote_files = [f for f in remote_files if NativeBinaryManager.parse_semver(f) is not None] # output: ["0.0.3-libikv.so", "0.0.5-libikv.so"]

        if len(remote_files) == 0:
            return None, None

        # pick highest semver
        highest_semver = NativeBinaryManager.parse_semver(remote_files[0])
        highest_semver_file = remote_files[0]
        for file in remote_files:
            if compare_semver(NativeBinaryManager.parse_semver(file), highest_semver) > 0:
                highest_semver = NativeBinaryManager.parse_semver(file)
                highest_semver_file = file

        return (highest_semver, "{}/{}".format(platform_prefix, highest_semver_file))
    
    def _download_remote_dll(self, object_key: str) -> Optional[str]:
        """
        Returns full file path to local bin after downloading it from remote.
        Also deletes older local binaries.
        """
        # delete existing binaries
        NativeBinaryManager._delete_files_in_directory("{}/bin".format(self.mount_dir))

        # ex. release/mac-aarch64/0.0.5-libiky.dylib -> 0.0.5-libiky.dylib
        remote_dll_filename = object_key.split("/")[-1]
        # ex. /path/to/mount/bin/0.0.5-libiky.dylib
        local_dll_path = "{}/bin/{}".format(self.mount_dir, remote_dll_filename)

        # download from s3 in chunks
        response = self.s3_client.get_object(Bucket=BUCKET_NAME, Key=object_key)
        if response is None or response['Body'] is None:
            return None

        with open(local_dll_path, 'wb') as local_dll_file:
            for chunk in response['Body'].iter_chunks(CHUNK_SIZE):
                local_dll_file.write(chunk)
        
        return local_dll_path

    def _delete_files_in_directory(directory):
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            if os.path.isfile(file_path):
                os.remove(file_path)

    def parse_semver(filename: str) -> Optional[str]:
        # input: 0.0.3-libikv.so, output: 0.0.3
        # input: 0.0.4, output: 0.0.4
        # input: libikv.so, output: None
        if len(filename) == 0: return None
        
        parts = filename.split("-")
        if not parts: return None
        
        if re.match(SEMVER_PATTERN, parts[0]) is not None: return parts[0]
        return None

    def platform_s3_prefix() -> Optional[str]:
        # return: release/{mac|linux|windows}-{aarch64|x86_64|tbd}
        raw_platform_os = str(platform.system()).lower()
        raw_platform_machine = str(platform.machine()).lower()

        if raw_platform_os == "linux":
            if raw_platform_machine == "x86_64" or raw_platform_machine == "amd64":
                return "release/linux-x86_64"
            elif raw_platform_machine == "arm64" or raw_platform_machine == "aarch64":
                return "release/linux-aarch64"
        elif raw_platform_os == "darwin" or raw_platform_os == "mac":
            if raw_platform_machine == "arm64" or raw_platform_machine == "aarch64":
                return "release/mac-aarch64"
        
        return None
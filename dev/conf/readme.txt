# readme.txt

This folder contains static configuration and resource files.

DO NOT EDIT THESE FILES UNLESS YOU ARE MAKING CHANGES THAT WILL
GLOBALLY APPLY TO ALL SDLC ENVIRONMENTS.

All conf folder INI and CFG files can be overridden by creating
identically named files in local/ with changes specific to your
current environment. Only the [section] and key=value settings
that are being changed need to be placed in the local copies of
these master files. The other settings will be inherited from the
master files in conf/.

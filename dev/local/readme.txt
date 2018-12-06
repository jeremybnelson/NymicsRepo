# readme.txt

This folder contains SDLC specific customizations.

To prepare UDP applications to run in a new environment:

1. Use the bootstrap.cmd to copy the current environment's
   s3_admin_*/log_terraform_*.ini to local/bootstrap.ini.

   You must specify the environment name-type as a parameter
   to the bootstrap command.

   bootstrap <sandbox-name | dev | uat | prod>

2. All conf folder INI and CFG files can be overridden by creating
   identically named files in local/ with changes specific to your
   current environment. Only the [section] and key=value settings
   that are being changed need to be placed in the local copies of
   these master files. The other settings will be inherited from the
   master files in conf/. DO NOT CUSTOMIZE FILES IN CONF.








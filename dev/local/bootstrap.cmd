@echo off
:
: bootstrap.cmd {sdlc}
:
: Copy the specified environment's log_terraform_*.ini file to bootstrap.ini.
:
: {sdlc} = sandbox-{name}, dev, uat, prod

if %1.==. echo SDLC env name required, eg. dev, uat, prod or sandbox-{name}
if %1.==. goto END

cmd /c aws s3 cp s3://aws-udp-s3-admin-%1/bootstrap/log/ . --recursive --exclude "*" --include "*.ini" > nul
move /y log_terraform_*.ini bootstrap.ini > nul
dir bootstrap.ini | find "bootstrap.ini"

:END

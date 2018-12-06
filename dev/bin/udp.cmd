@echo off
:
: udp.cmd {parameters}
:
: UPD command-launch script that titles, sizes and colors console, sets prompt, launches script.
:
: Usage examples:
:
: REM just configure the appearance of a specific environment without running a specific script
: udp
:
: REM run capture in dev console environment
: udp project_capture_amp_sales --nowait
:
: REM run archive in uat console environment
: udp project_archive
:

: TODO: 2018-09-27 repeat block of %DOCKER_* tests for use in non-Docker env
: TODO: VM_ENVIRONMENT_TYPE = sandbox, dev, uat, prod, or mixed (eg, capture VM hosting multiple Docker environment type containers)
: BETTER: Allow VM_ENVIRONMENT_TYPE to be overridden with a local file like env.ini that contains root folder's VM and Docker environment types ???
: TODO: Insert the env type (via DOCKER or non-Docker env) in prompt and title for all consoles launched from a root folder ???

: order directory listings by ext then name
set dircmd=/oen

: set console title to {server}:({sdlc}) {script} {parameters} (path={current-directory})
: ref: https://ss64.com/nt/title.html
if %1.==. title [%computername%]:(%0) (path=%~dp0)
if not %1.==. title [%computername%]:(%0) %1 (path=%~dp0)
if not %2.==. title [%computername%]:(%0) %1 %2 (path=%~dp0)
if not %3.==. title [%computername%]:(%0) %1 %2 %3 (path=%~dp0)
if not %4.==. title [%computername%]:(%0) %1 %2 %3 %4 (path=%~dp0)
if not %5.==. title [%computername%]:(%0) %1 %2 %3 %4 %5 (path=%~dp0)
if not %6.==. title [%computername%]:(%0) %1 %2 %3 %4 %5 %6 (path=%~dp0)
if not %7.==. title [%computername%]:(%0) %1 %2 %3 %4 %5 %6 %7 (path=%~dp0)
if not %8.==. title [%computername%]:(%0) %1 %2 %3 %4 %5 %6 %7 %8 (path=%~dp0)
if not %9.==. title [%computername%]:(%0) %1 %2 %3 %4 %5 %6 %7 %8 %9 (path=%~dp0)

: size console window to optimize screen space for multiple console windows
: ref: https://ss64.com/nt/mode.html
mode 100, 10

: set background/foreground colors by env: dev (white/black), uat (yellow/black), prod (red/white)
: ref: https://ss64.com/nt/color.html
if %DOCKER_CONTAINER_ENVIRONMENT_TYPE%.==sandbox. color f0
if %DOCKER_CONTAINER_ENVIRONMENT_TYPE%.==dev.     color f0
if %DOCKER_CONTAINER_ENVIRONMENT_TYPE%.==uat.     color e0
if %DOCKER_CONTAINER_ENVIRONMENT_TYPE%.==prod.    color cf

: set prompt to {server}:({sdlc}) {current-directory}
: ref: https://ss64.com/nt/prompt.html
prompt [%computername%]:(%0) $P$G

: run the actual app with parameters
%1 %2 %3 %4 %5 %6 %7 %8 %9

: script exit point
:DONE

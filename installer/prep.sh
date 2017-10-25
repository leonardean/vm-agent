if [ ! -z "$1" -a "$1" != " " ]; then
    echo "Creating installer for: $1"

    cp kiistarter.$1.bat kiistarter.bat
    cp kiiconfig.$1.json kiiconfig.json
    cp service.$1.json service.json

    # concatenate the files into the install command
    cat install-prefix.$1.cmd kii-agent-install-template.cmd > kii-agent-install.cmd

    echo "Done!"

else
	echo "No template passed"
fi
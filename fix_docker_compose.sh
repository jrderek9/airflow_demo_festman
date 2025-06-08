#!/bin/bash

# Remove the version line from docker-compose.yml
echo "Fixing docker-compose.yml warning..."
sed -i '' '1d' docker-compose.yml 2>/dev/null || sed -i '1d' docker-compose.yml

echo "✅ Fixed! The warning should no longer appear."
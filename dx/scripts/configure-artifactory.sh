#!/bin/sh

# Configure Artifactory for Maven
mkdir -p "$HOME/.m2"
cat >"$HOME/.m2/settings.xml" <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<settings xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.2.0 http://maven.apache.org/xsd/settings-1.2.0.xsd" xmlns="http://maven.apache.org/SETTINGS/1.2.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <servers>
    <server>
      <username>${env.ARTIFACTORY_USER}</username>
      <password>${env.ARTIFACTORY_TOKEN}</password>
      <id>central</id>
    </server>
    <server>
      <username>${env.ARTIFACTORY_USER}</username>
      <password>${env.ARTIFACTORY_TOKEN}</password>
      <id>snapshots</id>
    </server>
  </servers>
  <profiles>
    <profile>
      <repositories>
        <repository>
          <snapshots>
            <enabled>false</enabled>
          </snapshots>
          <id>central</id>
          <name>spire-maven-virtual</name>
          <url>https://artifacts.i.mercedes-benz.com/artifactory/spire-maven-virtual</url>
        </repository>
        <repository>
          <snapshots />
          <id>snapshots</id>
          <name>spire-maven-virtual</name>
          <url>https://artifacts.i.mercedes-benz.com/artifactory/spire-maven-virtual</url>
        </repository>
      </repositories>
      <pluginRepositories>
        <pluginRepository>
          <snapshots>
            <enabled>false</enabled>
          </snapshots>
          <id>central</id>
          <name>spire-maven-virtual</name>
          <url>https://artifacts.i.mercedes-benz.com/artifactory/spire-maven-virtual</url>
        </pluginRepository>
        <pluginRepository>
          <snapshots />
          <id>snapshots</id>
          <name>spire-maven-virtual</name>
          <url>https://artifacts.i.mercedes-benz.com/artifactory/spire-maven-virtual</url>
        </pluginRepository>
      </pluginRepositories>
      <id>artifactory</id>
    </profile>
  </profiles>
  <activeProfiles>
    <activeProfile>artifactory</activeProfile>
  </activeProfiles>
</settings>
EOF

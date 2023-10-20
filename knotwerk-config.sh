export CODEARTIFACT_REPOSITORY_URL=`aws codeartifact get-repository-endpoint --domain knotwerk --domain-owner 356538622071 --repository Knotwerk --format pypi --query repositoryEndpoint --output text`
export CODEARTIFACT_AUTH_TOKEN=`aws codeartifact get-authorization-token --domain knotwerk --domain-owner 356538622071 --query authorizationToken --output text`
export CODEARTIFACT_USER=aws
poetry source add --priority=supplemental knotwerk https://knotwerk-356538622071.d.codeartifact.eu-central-1.amazonaws.com/pypi/Knotwerk/simple
poetry config repositories.knotwerk $CODEARTIFACT_REPOSITORY_URL
poetry config http-basic.knotwerk $CODEARTIFACT_USER $CODEARTIFACT_AUTH_TOKEN

echo "/etc/poetry/bin/poetry config http-basic.knotwerk $CODEARTIFACT_USER $CODEARTIFACT_AUTH_TOKEN" > knotwerk-docker-config.sh

# OpenAPI contract

The build generates OpenAPI 3.0 JSON from Java annotations on the existing request
handlers, parameter classes, and response JSON key constants. The reader, scanner,
and map-response model converter in this source set run only at build time.

Run from the repository root:

```
./gradlew :cruise-control:generateOpenApiJson
./gradlew :cruise-control:test --tests '*CruiseControlOpenApiReaderTest' --tests '*OpenApiRuntimeTest' --tests '*RequestParameterTest' --tests '*ResponseTest'
```

The generated file is `cruise-control/build/generated/openapi/openapi.json`.
`processResources` packages it at `webroot/openapi.json` for Swagger UI and the
Vert.x router. Do not edit the generated file or maintain a separate YAML contract.
Keep operation IDs stable and distinguish versioned response envelopes from nested records.

## Build the API wiki

With npm and `redoc-cli` available, run `./gradlew buildApiWiki`. This generates the
contract first, then bundles it into `target/api_wiki/redoc-static.html`.

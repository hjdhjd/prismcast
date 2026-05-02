/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * eslint.config.mjs: Linting configuration for PrismCast.
 */
import hbPluginUtils from "homebridge-plugin-utils/build/eslint-rules.mjs";

export default hbPluginUtils({

  allowDefaultProject: ["eslint.config.mjs"],

  // Defer dot-notation linting to the TS-aware rule so it stops fighting the tsconfig's noPropertyAccessFromIndexSignature: bracket access on index-signature
  // properties is required by tsc and must be allowed by ESLint.
  extraConfigs: [{

    files: ["src/**/*.ts"],
    rules: {

      "@typescript-eslint/dot-notation": [ "warn", { allowIndexSignaturePropertyAccess: true } ],
      "dot-notation": "off"
    }
  }],

  js: ["eslint.config.mjs"],
  ts: ["src/**/*.ts"]
});

/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * eslint.config.mjs: Linting configuration for PrismCast.
 */
import hbPluginUtils from "homebridge-plugin-utils/build/eslint-rules.mjs";

export default hbPluginUtils({

  allowDefaultProject: ["eslint.config.mjs"],
  js: ["eslint.config.mjs"],
  ts: ["src/**/*.ts"]
});

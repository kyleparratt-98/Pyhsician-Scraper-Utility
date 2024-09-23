export class ConsoleLogger {
  logPretty = (description, value = null) => {
    console.log(
      "\x1b[32m%s\x1b[0m",
      `
  ******************************************
  *                                        *
  *              (＾▽＾)                   *
  *                                        *
  ******************************************
  ${description}: ${value}
  `
    );
  };

  logBasic = (value) => {
    console.log("\x1b[33m%s\x1b[0m", value);
  };

  logError = (description, error = null) => {
    console.error(
      "\x1b[31m%s\x1b[0m",
      `
  ******************************************
  *                                        *
  *                (╯︵╰,)                 *
  *                                        *
  ******************************************
  ${description}
  `,
      error
    );
  };
}

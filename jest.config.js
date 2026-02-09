module.exports = {
  transform: {
    "^.+\\.tsx?$": [
      "ts-jest",
      {
        tsconfig: "tsconfig.test.json",
      },
    ],
  },
  testEnvironment: "node",
  testPathIgnorePatterns: ["/node_modules/", "/dist/"],
}

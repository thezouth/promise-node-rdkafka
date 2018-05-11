module.exports = {
    globals: {
        "ts-jest": {
            tsConfigFile: "tsconfig.json"
        }
    },
    moduleFileExtensions: [ "ts", "js" ],
    transform: { "^.+\\.tsx?$": "ts-jest" },
    testMatch: [ "**/*/__test__/*.spec.(ts|js)" ],
    testEnvironment: "node"
}
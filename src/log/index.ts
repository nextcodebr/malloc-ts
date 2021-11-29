import console from 'console'

const MAP = [
  console.log,
  console.debug,
  console.info,
  console.warn,
  console.error
]

export const enum Level {
  LOG = 0,
  DEBUG = 1,
  INFO = 2,
  WARN = 3,
  ERROR = 4
}

export const logg = (msg: string, level = Level.LOG) => {
  msg = `[${new Date().toISOString()}][${msg}]`

  MAP[level ?? Level.LOG](msg)
}

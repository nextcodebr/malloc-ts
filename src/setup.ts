import fs from 'fs'
import path from 'path'

const subpath = (p: string): string => path.join(__dirname, p)

const main = (): void => {
  const source = fs.readFileSync(subpath('/../package.json')).toString('utf-8')
  const pkg = JSON.parse(source)
  delete pkg.scripts
  delete pkg.devDependencies
  if (pkg?.main?.startsWith('dist/')) {
    pkg.main = pkg.main.slice(5)
  }
  fs.writeFileSync(subpath('/package.json'), Buffer.from(JSON.stringify(pkg, null, 2), 'utf-8'))
  // fs.writeFileSync(__dirname + '/version.txt', Buffer.from(sourceObj.version, 'utf-8'))
  // fs.copyFileSync(subpath('/../.npmignore'), subpath('/.npmignore'))
  fs.copyFileSync(subpath('/../.npmrc'), subpath('/.npmrc'))
}

main()

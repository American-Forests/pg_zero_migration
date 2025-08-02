import path from 'path';
import { fileURLToPath } from 'url';
import esbuild from 'esbuild';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const entryPoints = [
  path.join(__dirname, 'src/migration.ts'),
  // Include any other TypeScript files that are imported
];
const outDir = 'dist';

await esbuild.build({
  entryPoints,
  bundle: true,
  outdir: path.join(__dirname, outDir),
  platform: 'node',
  target: 'es2022',
  format: 'esm',
  minify: true,
  treeShaking: true,
  metafile: true,
  sourcemap: false,
  banner: {
    // workaround require bug https://github.com/evanw/esbuild/pull/2067#issuecomment-1324171716
    js: "import { createRequire } from 'module'; const require = createRequire(import.meta.url);",
  },
});

console.log('Build completed successfully!');

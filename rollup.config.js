import typescript from '@rollup/plugin-typescript';
import terser from '@rollup/plugin-terser';

const { NODE_ENV } = process.env;
const isProduction = !NODE_ENV || NODE_ENV === 'production';

export default {
  input: 'src/index.ts',
  output: [{
    file: 'dist/peerix.umd.js',
    format: 'umd',
    name: 'peerix',
  }, {
    file: 'dist/peerix.esm.js',
    format: 'esm',
  }],
  plugins: [
    typescript({ tsconfig: './tsconfig.json' }),
    isProduction && terser(),
  ].filter(Boolean),
};

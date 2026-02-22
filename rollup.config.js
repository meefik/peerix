import terser from '@rollup/plugin-terser';

const { NODE_ENV = 'production' } = process.env;

export default {
  input: 'src/index.js',
  output: [{
    file: 'dist/p2p.umd.js',
    format: 'umd',
    name: 'p2p',
  }, {
    file: 'dist/p2p.esm.js',
    format: 'esm',
  }],
  plugins: NODE_ENV === 'production' ? [terser()] : [],
};

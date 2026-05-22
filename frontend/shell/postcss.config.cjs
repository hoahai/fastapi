const tailwindcss = require("../apps/tradsphere/node_modules/tailwindcss");
const autoprefixer = require("../apps/tradsphere/node_modules/autoprefixer");

module.exports = {
  plugins: [tailwindcss(), autoprefixer()],
};

import React, { Fragment } from "react";
import ReactDOM from "react-dom/client";

import App from "./App";
import "./index.css";

const RootWrapper = import.meta.env.DEV ? Fragment : React.StrictMode;

ReactDOM.createRoot(document.getElementById("root")!).render(
  <RootWrapper>
    <App />
  </RootWrapper>,
);

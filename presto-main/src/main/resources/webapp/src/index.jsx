import React from "react";
import ReactDOM from "react-dom";
import {ClusterHUD} from "./components/ClusterHUD";
import {QueryInputBox} from "./components/QueryInputBox";
import {QueryList} from "./components/QueryList";
import {PageTitle} from "./components/PageTitle";

ReactDOM.render(
    <PageTitle title="Gourd Store" />,
    document.getElementById('title')
);

ReactDOM.render(
    <ClusterHUD />,
    document.getElementById('cluster-hud')
);

ReactDOM.render(
    <QueryInputBox />,
    document.getElementById('query-input')
);

ReactDOM.render(
    <QueryList />,
    document.getElementById('query-list')
);

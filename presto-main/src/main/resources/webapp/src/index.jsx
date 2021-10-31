import React from "react";
import ReactDOM from "react-dom";
import {PageTitle} from "./components/PageTitle";
import {PrestoInfo} from "./components/PrestoInfo";
import {SparkInfo} from "./components/SparkInfo";

ReactDOM.render(
    <PageTitle title="Gourd Store" />,
    document.getElementById('title')
);

ReactDOM.render(
    <PrestoInfo />,
    document.getElementById('presto-info')
);

ReactDOM.render(
    <SparkInfo />,
    document.getElementById('spark-info')
);


'use strict';

const functions = require('@google-cloud/functions-framework');

const firebaseFunctions = require('firebase-functions');
const admin = require('firebase-admin');
admin.initializeApp();
const firestore = admin.firestore();

// Retrieve aggregated data from Crossref (or DataCite) and OpenCitations
// deploy with: "gcloud functions deploy pure-publications --gen2 --runtime=nodejs18 --region=europe-west1 --source=. --entry-point=pure-publications --trigger-http --allow-unauthenticated"
functions.http('pure-publications', async (req, res) => {
    const timeStart = new Date().getTime();
    const logEntry = { severity: "INFO" };

    const doi = req.query.doi.toLowerCase();
    const noCache = req.query.noCache === "true";
    if (!doi) {
        res.status(400).send('Missing DOI parameter');
        return;
    }
    logEntry.doi = doi;

    res.set('Content-Type', 'application/json');
    res.set('Access-Control-Allow-Origin', '*');
    res.set('Access-Control-Allow-Methods', 'GET');

    if (req.method === "OPTIONS") {
        // stop preflight requests here
        res.status(204).send('');
        return;
    }

    // Convert forward to backward slashes in doi
    const doi2 = doi.replace(/\//g, '\\');
    const doiRef = firestore.collection('pure-publications').doc(doi2);
    const doiDoc = await doiRef.get();
    let data = { doi: doi };
    // return cached data if available and not expired
    if (!noCache && doiDoc.exists && doiDoc.data().expireAt.toDate() > new Date()) {
        logEntry.tag = "cache-hit";
        data = doiDoc.data().data;
    } else {
        logEntry.tag = noCache ? "cache-disabled" : "cache-miss";

        // load metadata from Crossref 
        const timeStartCrossref = new Date().getTime();
        const reponseCrossref = await fetch(`https://api.crossref.org/v1/works/${doi}?mailto=fabian.beck@uni-bamberg.de`);
        const dataCrossref = reponseCrossref.status === 200 ? (await reponseCrossref.json())?.message : null;
        logEntry.crossref = { status: reponseCrossref.status, processingTime: new Date().getTime() - timeStartCrossref };
        // load metadata from DataCite (as additional source)
        let dataDataCite = null;
        if (!dataCrossref) {
            const timeStartDataCite = new Date().getTime();
            const reponseDataCite = await fetch(`https://api.datacite.org/dois/${doi}`);
            dataDataCite = reponseDataCite.status === 200 ? (await reponseDataCite.json())?.data : null;
            logEntry.dataCite = { status: reponseDataCite.status, processingTime: new Date().getTime() - timeStartDataCite };
        }
        // merge metadata from Crossref and Datacite
        data.title = dataCrossref?.title?.[0]
            || dataDataCite?.attributes?.titles?.[0]?.title;
        data.subtitle = dataCrossref?.subtitle?.[0];
        data.year = dataCrossref?.published?.['date-parts']?.[0]?.[0]
            || dataDataCite?.attributes?.publicationYear
            || doi.match(/\.((19|20)\d\d)\./)?.[1];
        data.author = dataCrossref?.author?.reduce((acc, author) => acc + author.family
            + (author.given ? ", " + author.given : "")
            + (author.ORCID ? ", " + author.ORCID.replace(/http(s?):\/\/orcid.org\//g, "") : "")
            + "; ", "").slice(0, -2)
            || dataDataCite?.attributes?.creators?.reduce((acc, author) => acc + author.name + "; ", "").slice(0, -2);
        data.container = dataCrossref?.["container-title"]?.[0]
            || dataDataCite?.attributes?.relatedItems?.[0]?.titles?.[0]?.title;
        data.volume = dataCrossref?.volume;
        data.issue = dataCrossref?.issue;
        data.page = dataCrossref?.page;
        data.abstract = dataCrossref?.abstract
            || dataDataCite?.attributes?.descriptions?.[0]?.description;
        data.reference = dataCrossref?.reference?.reduce((acc, reference) =>
            (reference.DOI ? acc + reference.DOI + "; " : acc), "").slice(0, -2);

        // load citations from OpenCitations
        const timeStartOpenCitations = new Date().getTime();
        data.citation = "";
        const reponseOCCitations = await fetch(`https://opencitations.net/index/coci/api/v1/citations/${doi}`, {
            headers: {
                authorization: "aa9da96d-3c7b-49c1-a2d8-1c2d01ae10a5",
            }
        });
        const dataOCCitations = reponseOCCitations.status === 200 ? (await reponseOCCitations.json()) : null;
        dataOCCitations?.forEach(refernce => {
            data.citation += refernce.citing + "; ";
        });
        logEntry.openCitations = {
            statusCitations: reponseOCCitations.status,
            processingTime: new Date().getTime() - timeStartOpenCitations
        };

        // remove undefined/empty properties from data
        Object.keys(data).forEach(key => (data[key] === undefined || data[key] === '') && delete data[key]);
        // store data in cache with expiration date
        const expireDate = new Date();
        expireDate.setDate(expireDate.getDate() + 30);
        await doiRef.set({ expireAt: expireDate, data: data, source: dataDataCite ? "DataCite" : "Crossref" });
    }

    // log request
    logEntry.title = data.title;
    logEntry.processingTime = new Date().getTime() - timeStart;
    console.log(JSON.stringify(logEntry));

    // return data
    res.send(data);
});
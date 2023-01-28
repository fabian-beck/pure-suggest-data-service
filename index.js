
'use strict';

const functions = require('@google-cloud/functions-framework');

const firebaseFunctions = require('firebase-functions');
const admin = require('firebase-admin');
admin.initializeApp();
const firestore = admin.firestore();

// Retrieve aggregated data from OpenCitations and Crossref
// deploy with: "gcloud functions deploy pure-publications --gen2 --runtime=nodejs18 --region=europe-west1 --source=. --entry-point=pure-publications --trigger-http --allow-unauthenticated"
functions.http('pure-publications', async (req, res) => {
    const doi = req.query.doi;
    if (!doi) {
        res.status(400).send('Missing DOI parameter');
        return;
    }
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
    // return cached data if available and not expired
    if (doiDoc.exists && doiDoc.data().expireAt.toDate() > new Date()) {
        res.send(doiDoc.data().data);
    } else {
        const data = { doi: doi };
        // load data from OpenCitations
        const urlOpenCitations = `https://opencitations.net/index/coci/api/v1/metadata/${doi}`;
        const reponseOpenCitations = await fetch(urlOpenCitations, {
            headers: {
                authorization: "aa9da96d-3c7b-49c1-a2d8-1c2d01ae10a5",
            }
        });
        const dataOpenCitations = reponseOpenCitations.status === 200 ? (await reponseOpenCitations.json())[0] : null;
        // load data from Crossref
        const urlCrossref = `https://api.crossref.org/v1/works/${doi}?mailto=fabian.beck@uni-bamberg.de`
        const reponseCrossref = await fetch(urlCrossref);
        const dataCrossref = reponseCrossref.status === 200 ? await reponseCrossref.json() : null;
        // merge data
        data.title = dataCrossref?.message?.title?.[0] || dataOpenCitations?.title;
        data.subtitle = dataCrossref?.message?.subtitle?.[0];
        data.year = dataOpenCitations?.year || dataCrossref?.message?.created?.['date-parts']?.[0]?.[0] || doi.match(/\.((19|20)\d\d)\./)?.[1];
        data.author = dataOpenCitations?.author;
        data.container = dataOpenCitations?.source_title;
        data.volume = dataOpenCitations?.volume;
        data.issue = dataOpenCitations?.issue;
        data.page = dataOpenCitations?.page;
        data.oaLink = dataOpenCitations?.oa_link;
        data.reference = dataOpenCitations?.reference;
        data.citation = dataOpenCitations?.citation;
        data.abstract = dataCrossref?.message?.abstract;
        // remove undefined/empty properties from data
        Object.keys(data).forEach(key => (data[key] === undefined || data[key] === '') && delete data[key]);
        // store data in cache with expiration date
        const expireDate = new Date();
        expireDate.setDate(expireDate.getDate() + 30);
        await doiRef.set({ expireAt: expireDate, data: data });
        // return data
        res.send(data);
    }
});
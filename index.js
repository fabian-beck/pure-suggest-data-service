
'use strict';

const functions = require('@google-cloud/functions-framework');

const firebaseFunctions = require('firebase-functions');
const admin = require('firebase-admin');
admin.initializeApp();
const firestore = admin.firestore();

// Retrieve aggregated data from OpenCitations and CrossRef
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

        // load metadata from Crossref 
        const reponseCrossref = await fetch(`https://api.crossref.org/v1/works/${doi}?mailto=fabian.beck@uni-bamberg.de`);
        const dataCrossref = reponseCrossref.status === 200 ? (await reponseCrossref.json())?.message : null;
        data.title = dataCrossref?.title?.[0];
        data.subtitle = dataCrossref?.subtitle?.[0];
        data.year = dataCrossref?.created?.['date-parts']?.[0]?.[0] || doi.match(/\.((19|20)\d\d)\./)?.[1];
        data.author = dataCrossref?.author?.reduce((acc, author) => acc + author.family + ", " + author.given + "; ", "").slice(0, -2);
        data.container = dataCrossref?.["container-title"]?.[0];
        data.volume = dataCrossref?.volume;
        data.issue = dataCrossref?.issue;
        data.page = dataCrossref?.page;
        data.abstract = dataCrossref?.abstract;

        // load refernces/citations from OpenCitations
        data.reference = "";
        const reponseOCReferences = await fetch(`https://opencitations.net/index/coci/api/v1/references/${doi}`, {
            headers: {
                authorization: "aa9da96d-3c7b-49c1-a2d8-1c2d01ae10a5",
            }
        });
        const dataOCReferences = reponseOCReferences.status === 200 ? (await reponseOCReferences.json()) : null;
        dataOCReferences?.forEach(refernce => {
            data.reference += refernce.cited + "; ";
        });
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
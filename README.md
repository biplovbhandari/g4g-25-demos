# g4g-25-demos

### Background

Deep learning is becoming increasingly common for remote sensing applications. In deep learning applications of remote sensing, image classification and image segmentation serve different purposes and practitioners must make model architectural decisions early on to best serve their end goal. 

So far we have seen embeddings as a service (EaaS) take off as an idea, championed by organizations like EarthGenome, others. The fundamental claim is that if an EO foundation model has learned useful enough representations of its remote sensing input data (typically spanning the glob), then the result of simply passing remote sensing data through it - an embedding - should be generally useful enough to be directly applied to your end goal task with little to no additional modeling overhead on your side (simple ML typically applied). 

So far, the typical use cases highlighted are image classification. Examples include vector search ("show me areas of gold mining in the Amazon") and change detection ("Show me areas that changed from forest to urban development").

 A large proportion (perhaps the majority) of remote sensing applications continue to require the specificity of image segmentation in order to quantify land form, changes, etc for important objectives like land use planning, climate and forestry reporting.
 
 Currently (check..?), EO foundation models either encode pixels or image patches, rendering their embeddings theoretically only directly useful for either image segmentation or classification tasks, respectively. In their published papers, all of these models are tested on both image classification and segmentation, however, further deep learning is done by the model developers to turn their model's patch-based embeddings into pixel-based results, or their model's pixel-based embeddings into patch-based results - typically known as the decoder portion of a model's entire architecture. If EaaS is to continue to mature into an actionable remote sensing application, we'd like to know if this divide can be bridged by the community. 
 
 Ideally an end user doesn't need to know which model(s) are used behind the application they are using. If there are in fact multiple models behind the curtain, we'll let the technical folk worry about that...

 So...technical folk..
 
 To practitioners: Wouldn't it be nice to have one model producing results for all of your use cases you serve in your application, or is that a pipe dream? Could some data engineering on the model output one day replace that extra deep learning overhead?

 To model developers: Is it a pipe dream or are we close to a model's equal utility for all downstream task types? Could we ever truly remove the additional modeling overhead implied for EaaS products?

### Experiments

Recently, Earth Genome released global 2024 embeddings produced with a model that natively encodes image patch embeddings, while Google EFM released their pixel-based global EFM embeddings dataset (annual datasets for xx - 2024).

Our experiment is two-fold:
1. Can a pixel-based embedding dataset be engineered to perform comparatively on image classification tasks against a patch-based embedding dataset? What's involved and how feasible is it to do?
2. Can a patch-based embedding dataset be engineered to perform comparatively on image segmentation tasks against a pixel-based embedding dataset? What's involved and how feasible is it to do?

In the process we hope to at least come away with some newly discovered considerations, challenges, and opinions about what's next for brdiging the pixel/patch divide. Our takeaways will obviously be limited in context to our results (this one model of each type among many other experiment decisions), so take them with a grain of salt.

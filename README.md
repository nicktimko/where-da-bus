# Where oh where could my bus be...

Collect bus tracker data to develop a model that can give an estimated arrival time. The intercampus seems to show up anywhere from 2 minutes early to 20 minutes late. `:(`

The API to hit is at `https://maps.northwestern.edu/api/shuttles`, it gives a JSON. The page that uses it "properly" (https://maps.northwestern.edu/shuttle/intercampus) hits the endpoint every 5 seconds (and also *4 times* when the page loads, returning the *exact same data each time*...) so we'll do it 10 and call ourselves polite.
